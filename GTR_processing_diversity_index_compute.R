# ==============================================================================
# OpenAlex Author-Institution Matching Pipeline
# Optimised for large-scale PostgreSQL queries
# ==============================================================================

# ---- Load Libraries ----
# List of packages
packages <- c(
  "dplyr", "purrr", "stringr", "dbplyr", "DBI", "furrr",
  "stringdist", "arrow", "readr", "RPostgres", "tidyr", "DescTools"
)

# Install missing packages
installed <- installed.packages()[,"Package"]
to_install <- setdiff(packages, installed)

if(length(to_install) > 0) {
  install.packages(to_install)
}

# Load packages
lapply(packages, library, character.only = TRUE)


# ---- Parameters ----
N_WORKERS <- 4
BATCH_SIZE <- 50000  # For very large publication datasets

# === INPUT YEAR RANGE =====
year_range <- c(2021, 2024)
# ==========================

# ---- 1. Load GTR+ Data ----
# read GTR+ publication data
publication  <- {
  publication <- fread("./gtr_publications_2025_11.csv")
  setnames(publication, c(colnames(publication)[-1], ""))
  publication <- publication[, -ncol(publication), with = FALSE]
}

# read GTR+ participants/projects data
participants <- read_excel('./gtr+_projects_participants_2025_12.xlsx', sheet = 2)

# connect projects with publications
final_RC_publications_short <- publication %>%
  full_join(
    participants_with_RC %>%
      mutate(base_ref_part = sub("/\\d+$", "", reference)),
    by = c("ProjectReference" = "reference")
  ) %>%
  filter(
    grepl("RC$", funder_l0) |
      grepl("RC$", funder_l1) |
      grepl("RC$", funder_l2) |
      grepl("RC$", funder_l3)
  ) %>%
  select(
    FundingOrg, ProjectReference, LeadRO, Department, PublicationType,
    Author, Year, Title, JournalName, Volume, Issue, Pages, DOI, PMID,
    URL, GtRPublicationURL, funder_l0, funder_l1, funder_l2, funder_l3,
    start, start_cy, start_fy, end, org_name, org_role, org_country_code,
    org_sector, org_ror_id, org_ukprn
  )
saveRDS(final_RC_publications_short, file = "./project_publications_short.rds")
message("       Loaded ", nrow(final_RC_publications_short), " total records")

# ---- 2. Connect to Database ----
source("/db_connect.R")

if (is.null(con)) {
  stop("Failed to establish database connection. Exiting.")
}

dbExecute(con, "SET random_page_cost = 4;")
dbExecute(con, "SET effective_io_concurrency = 4;")
dbExecute(con, "SET jit = off;")
dbExecute(con, "SET work_mem = '1GB';")
dbExecute(con, "SET effective_cache_size = '150GB';")
dbExecute(con, "SET max_parallel_workers_per_gather = 4;")

# Track temp tables for cleanup
temp_tables_created <- c()

# Register cleanup on exit (runs even if script fails)
on.exit({
  message("\n[CLEANUP] Removing temporary tables...")
  cleanup_temp_tables(con, temp_tables_created)
  dbDisconnect(con)
  message("[CLEANUP] Database connection closed.")
}, add = TRUE)


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

#' Extract blocking token from organisation name
#' Removes common stopwords and returns first meaningful token
#'
#' @param org_name Character vector of organisation names
#' @return Character vector of first meaningful tokens
extract_block_token <- function(org_name) {
  stopwords_uni <- c(
    "university", "univ", "college", "school", "institute", "inst",
    "universite", "université", "universitat", "universität",
    "universidad", "universidade", "universita", "università",
    "universiteit", "universitet", "universitete",
    "of", "the", "de", "del", "la", "le", "der", "di", "da"
  )
  
  map_chr(org_name, function(x) {
    if (is.na(x) || x == "") return(NA_character_)
    tokens <- str_split(x, "\\s+")[[1]]
    tokens_clean <- tokens[!str_to_lower(tokens) %in% stopwords_uni]
    if (length(tokens_clean) == 0) return(NA_character_)
    
    # Take first 2 tokens if available, otherwise just the first one
    if (length(tokens_clean) == 1) {
      paste(tokens_clean, "")  # pad with a space if only one token
    } else {
      paste(tokens_clean[1:2], collapse = " ")
    }
  })
}


#' Clean up temporary tables from database
#'
#' @param con Database connection
#' @param table_names Character vector of temp table names
cleanup_temp_tables <- function(con, table_names) {
  for (tbl_name in table_names) {
    tryCatch({
      dbExecute(con, paste0("DROP TABLE IF EXISTS ", tbl_name))
      message("  Dropped: ", tbl_name)
    }, error = function(e) {
      warning("Could not drop ", tbl_name, ": ", e$message)
    })
  }
}


#' Normalise DOI string for matching
#'
#' @param doi Character vector of DOIs
#' @return Character vector of normalised DOIs
normalise_doi <- function(doi) {
  doi |>
    str_to_lower() |>
    str_trim() |>
    str_remove("^https?://doi\\.org/") |>
    str_remove("^doi:")
}

# ==============================================================================
# MAIN PIPELINE
# ==============================================================================

run_matching_pipeline <- function(year = YEAR, batch_size = BATCH_SIZE) {
  
  message("\n", strrep("=", 60))
  message("OpenAlex Author-Institution Matching Pipeline")
  message("Year: ", year, " | Started: ", Sys.time())
  message(strrep("=", 60), "\n")
  
  # ---- 3. Prepare Local Publication Data ----
  message("[3/10] Preparing publication data for year ", year, "...")
  
  temp_pub <- final_RC_publications_short |>
    filter(Year == year, !is.na(DOI), DOI != "") |>
    transmute(
      grant = ProjectReference,
      doi_clean = normalise_doi(DOI),
      org_name = org_name,
      org_country_code = org_country_code,
      org_ror_id = org_ror_id,
      has_ror = !is.na(org_ror_id) &
        str_trim(org_ror_id) != "" &
        str_trim(org_ror_id) != "NA"
    ) |>
    distinct()
  
  # Add blocking tokens locally (faster than in DB)
  temp_pub <- temp_pub |>
    mutate(org_block = extract_block_token(org_name))
  
  n_pubs <- nrow(temp_pub)
  n_with_ror <- sum(temp_pub$has_ror)
  n_without_ror <- n_pubs - n_with_ror
  
  n_local_dois <- temp_pub %>%
    summarise(n_distinct_doi = n_distinct(doi_clean)) %>%
    pull(n_distinct_doi)
  
  message("       Records for ", year, ": ", n_pubs)
  message("       With ROR: ", n_with_ror, " | Without ROR: ", n_without_ror)
  
  # ---- 4. Upload Temp Table ----
  message("[4/10] Uploading temp table to database...")
  
  dbWriteTable(
    con,
    "temp_publication",
    as.data.frame(temp_pub),
    temporary = TRUE,
    overwrite = TRUE,
    row.names = FALSE
  )
  temp_tables_created <- c(temp_tables_created, "temp_publication")
  
  # Create indexes on temp table (these are fast for small tables)
  dbExecute(con, "CREATE INDEX idx_temp_doi ON temp_publication(doi_clean)")
  dbExecute(con, "CREATE INDEX idx_temp_ror ON temp_publication(org_ror_id) WHERE has_ror")
  dbExecute(con, "CREATE INDEX idx_temp_country ON temp_publication(org_country_code)")
  dbExecute(con, "ANALYZE temp_publication")
  
  message("       Temp table created with indexes")
  
  # ---- 5. Create Lazy Table References ----
  message("[5/10] Creating database table references...")
  
  temp_pub_tbl <- tbl(con, "temp_publication")
  
  # Works table: filter by year and create cleaned DOI
  works_tbl <- tbl(con, in_schema("openalex", "works")) |>
    filter(publication_year == year, !is.na(doi)) |>
    mutate(
      doi_clean = sql("LOWER(REPLACE(doi, 'https://doi.org/', ''))")
    ) |>
    select(work_id = id, doi_clean, publication_year)
  
  # Works authorships: only rows with institutions
  works_authorships_tbl <- tbl(con, in_schema("openalex", "works_authorships")) |>
    filter(!is.na(institution_id)) |>
    select(work_id, author_id, institution_id)
  
  # Institutions
  institutions_tbl <- tbl(con, in_schema("openalex", "institutions")) |>
    select(
      institution_id = id,
      inst_display_name = display_name,
      ror,
      country_code
    )
  
  # Authors
  authors_tbl <- tbl(con, in_schema("openalex", "authors")) |>
    select(author_id = id, author_name = display_name)
  
  message("       Table references created")
  
  # ---- 6. Join Publications to Works (Materialise) ----
  message("[6/10] Joining publications to works (materialising)...")
  
  # This is the key optimisation: materialise the DOI join result
  # so we don't repeat the expensive string operation
  pub_works_query <- temp_pub_tbl |>
    inner_join(works_tbl, by = "doi_clean")
  
  # Show query for debugging
  message("       Query plan:")
  pub_works_query |> show_query() |> capture.output() |> head(10) |> walk(~message("         ", .x))
  
  # Materialise to temp table
  pub_works_tbl <- pub_works_query |>
    compute(name = "temp_pub_works", temporary = TRUE,  overwrite = TRUE)
  
  temp_tables_created <- c(temp_tables_created, "temp_pub_works")
  
  # Index the materialised table
  dbExecute(con, "CREATE INDEX idx_tpw_work ON temp_pub_works(work_id)")
  dbExecute(con, "CREATE INDEX idx_tpw_ror ON temp_pub_works(org_ror_id) WHERE has_ror")
  dbExecute(con, "ANALYZE temp_pub_works")
  
  # Distinct DOIs in the materialised table
  n_matched_dois <- pub_works_tbl %>%
    summarise(n_distinct_doi = n_distinct(doi_clean)) %>%
    collect() %>%
    pull(n_distinct_doi)
  
  message("       DOIs matched to works: ", n_matched_dois, 
          " | Total DOIs in local data: ", n_local_dois)
  
  # ---- 7. Exact ROR Matches ----
  message("[7/10] Finding exact ROR matches...")
  
  exact_matches_query <- tbl(con, "temp_pub_works") |>
    filter(has_ror) |>
    inner_join(works_authorships_tbl, by = "work_id") |>
    inner_join(
      institutions_tbl |> filter(!is.na(ror)),
      by = "institution_id"
    ) |>
    filter(ror == org_ror_id) |>
    inner_join(authors_tbl, by = "author_id") |>
    select(
      grant,
      work_id,
      author_id,
      author_name,
      doi_clean,
      inst_display_name,
      org_name,
      institution_id,
      ror
    ) |>
    distinct()
  
  exact_matches <- exact_matches_query |> collect()
  
  message("       Exact ROR matches: ", nrow(exact_matches))
  
  # ---- 8. Fuzzy Matching (Missing ROR) ----
  message("[8/10] Preparing fuzzy match candidates...")
  
  fuzzy_candidates_query <- tbl(con, "temp_pub_works") |>
    filter(!has_ror) |>
    inner_join(works_authorships_tbl, by = "work_id") |>
    inner_join(institutions_tbl, by = "institution_id") |>
    filter(country_code == org_country_code) |>  # Pre-filter by country
    inner_join(authors_tbl, by = "author_id") |>
    select(
      grant,
      work_id,
      author_id,
      author_name,
      doi_clean,
      inst_display_name,
      org_name,
      org_block,
      institution_id,
      country_code
    ) |>
    distinct()
  
  # Materialise fuzzy candidates
  fuzzy_candidates_tbl <- fuzzy_candidates_query |>
    compute(name = "temp_fuzzy_candidates", temporary = TRUE,  overwrite = TRUE)
  
  temp_tables_created <- c(temp_tables_created, "temp_fuzzy_candidates")
  
  # Collect to local for string operations
  fuzzy_candidates_local <- fuzzy_candidates_tbl |> collect()
  
  message("       Fuzzy match candidates: ", nrow(fuzzy_candidates_local))
  
  if (nrow(fuzzy_candidates_local) > 0) {
    
    # Add institution blocking tokens locally
    fuzzy_candidates_local <- fuzzy_candidates_local |>
      mutate(inst_block = extract_block_token(inst_display_name))
    
    # ---- 8a. Token Block Matches ----
    token_matches <- fuzzy_candidates_local |>
      filter(!is.na(inst_block), !is.na(org_block)) |>
      filter(str_to_lower(inst_block) == str_to_lower(org_block)) |>
      select(
        grant,
        work_id,
        author_id,
        author_name,
        doi_clean,
        inst_display_name,
        org_name,
        institution_id
      ) |>
      distinct()
    
    message("       Token block matches: ", nrow(token_matches))
    
  } else {
    token_matches <- tibble(
      grant = character(),
      work_id = character(),
      author_id = character(),
      author_name = character(),
      doi_clean = character(),
      inst_display_name = character(),
      org_name = character(),
      institution_id = character()
    )
  }
  
  # ---- 9. String Distance Matching (Fallback) ----
  message("[9/10] Running string distance matching on unmatched records...")
  
  if (nrow(fuzzy_candidates_local) > 0 && nrow(token_matches) > 0) {
    
    unmatched <- fuzzy_candidates_local |>
      anti_join(
        token_matches,
        by = c("work_id", "author_id", "institution_id")
      )
    
  } else if (nrow(fuzzy_candidates_local) > 0) {
    unmatched <- fuzzy_candidates_local
  } else {
    unmatched <- tibble()
  }
  
  if (nrow(unmatched) > 0) {
    
    message("       Unmatched records for stringdist: ", nrow(unmatched))
    
    # Normalise names for comparison
    unmatched <- unmatched |>
      mutate(
        inst_norm = str_to_lower(str_squish(inst_display_name)),
        org_norm = str_to_lower(str_squish(org_name))
      )
    
    # Use parallel processing for large datasets
    if (nrow(unmatched) > 10000) {
      message("       Using parallel processing (", N_WORKERS, " workers)...")
      
      plan(multisession, workers = N_WORKERS)
      
      unmatched <- unmatched |>
        mutate(
          jw_dist = future_map2_dbl(
            inst_norm, org_norm,
            ~stringdist(.x, .y, method = "jw"),
            .progress = TRUE
          )
        )
      
      plan(sequential)
      
    } else {
      unmatched <- unmatched |>
        mutate(jw_dist = stringdist(inst_norm, org_norm, method = "jw"))
    }
    
    # Filter by threshold (Jaro-Winkler distance < 0.2 = good match)
    stringdist_matches <- unmatched |>
      filter(jw_dist < 0.2) |>
      select(
        grant,
        work_id,
        author_id,
        author_name,
        doi_clean,
        inst_display_name,
        org_name,
        institution_id,
        jw_dist
      ) |>
      distinct()
    
    message("       String distance matches (JW < 0.2): ", nrow(stringdist_matches))
    
  } else {
    stringdist_matches <- tibble(
      grant = character(),
      work_id = character(),
      author_id = character(),
      author_name = character(),
      doi_clean = character(),
      inst_display_name = character(),
      org_name = character(),
      institution_id = character(),
      jw_dist = numeric()
    )
    message("       No unmatched records for stringdist")
  }
  
  # ---- 10. Combine Results ----
  message("[10/10] Combining all matches...")
  
  final_matches <- bind_rows(
    exact_matches |> mutate(match_type = "exact_ror", jw_dist = NA_real_),
    token_matches |> mutate(match_type = "token_block", jw_dist = NA_real_),
    stringdist_matches |> mutate(match_type = "string_distance")
  ) |>
    # Remove potential duplicates
    distinct(work_id, author_id, institution_id, .keep_all = TRUE) |>
    # Clean up columns
    select(
      grant,
      work_id,
      author_id,
      author_name,
      doi_clean,
      org_name,
      institution_id,
      inst_display_name,
      match_type,
      jw_dist
    )
  
  # ---- Summary ----
  message("\n", strrep("=", 60))
  message("RESULTS SUMMARY")
  message(strrep("=", 60))
  message("Total matched author-institution pairs: ", nrow(final_matches))
  message("  - Exact ROR matches:      ", sum(final_matches$match_type == "exact_ror"))
  message("  - Token block matches:    ", sum(final_matches$match_type == "token_block"))
  message("  - String distance matches:", sum(final_matches$match_type == "string_distance"))
  message("DOIs matched to works: ", n_matched_dois, 
          " | Total DOIs in local data: ", n_local_dois)
  
  # generate summary/information table
  summary_info <- tibble(
    year = year,
    total_records = nrow(final_matches),
    exact_ror_match = sum(final_matches$match_type == "exact_ror"),
    token_block_match = sum(final_matches$match_type == "token_block"),
    string_distance_match = sum(final_matches$match_type == "string_distance"),
    dois_matched = n_matched_dois,
    dois_total = n_local_dois
  )
  
  # Return both the matches and summary
  invisible(list(
    matches = final_matches,
    summary = summary_info
  ))
  
}


# ==============================================================================
# RUN PIPELINE
# ==============================================================================

run_yearly_matching_parquet <- function(start_year,
                                        end_year,
                                        summary_file = NULL,
                                        parquet_file = NULL,
                                        yearly_folder = "yearly_parquet") {
  
  # ----------------------------
  # Auto-generate filenames if not provided
  # ----------------------------
  if (is.null(summary_file)) {
    summary_file <- paste0("author_institution_matches_summary_", 
                           start_year, "_", end_year, ".parquet")
  }
  
  if (is.null(parquet_file)) {
    parquet_file <- paste0("author_institution_matches_all_years_", 
                           start_year, "_", end_year, ".parquet")
  }
  
  # ----------------------------
  # 1. Run pipeline per year
  # ----------------------------
  summary_list <- list()
  
  for (year in start_year:end_year) {
    message("Running pipeline for year: ", year)
    
    res <- run_matching_pipeline(year = year)
    
    # store yearly summary
    summary_list[[as.character(year)]] <- res$summary
    
    # save individual year matches as Parquet
    arrow::write_parquet(res$matches, paste0("author_institution_matches_", year, ".parquet"))
  }
  
  # combine summaries
  summary_df <- bind_rows(summary_list)
  arrow::write_parquet(summary_df, summary_file)
  message("Saved combined summary: ", summary_file)
  
  # ----------------------------
  # 2. Combine all yearly Parquet files
  # ----------------------------
  files <- list.files(yearly_folder, pattern = "\\.parquet$", full.names = TRUE)
  
  all_matches <- purrr::map_dfr(files, function(f) {
    year <- str_extract(basename(f), "\\d{4}") |> as.integer()
    arrow::read_parquet(f) %>%
      mutate(year = year)
  })
  
  arrow::write_parquet(all_matches, parquet_file)
  message("Saved all matches as Parquet: ", parquet_file)
  
  return(list(
    summary = summary_df,
    matches = all_matches
  ))
}


res <- run_yearly_matching_parquet(start_year = min(year_range), end_year = max(year_range))



#---------------------------
# distribution table
grant_author_dist <- all_matches %>%
  group_by(grant) %>%
  summarise(
    n_authors = n_distinct(author_id),
    .groups = "drop"
  ) %>%
  count(n_authors, name = "n_grants") %>%
  arrange(n_authors) %>%
  mutate(
    pct = n_grants / sum(n_grants) * 100,
    cum_pct = cumsum(pct)
  )

grant_author_dist
#-------------------------------------


# '''''''''''''''''''''''''''''''''''''''''''''''''
# Index computation
# '''''''''''''''''''''''''''''''''''''''''''''''''

# Read data
# ----------------------------------
all_matches <- read_parquet("author_institution_matches_all_years_2021_2024.parquet")

all_matches <- all_matches %>% filter(match_type != "string_distance") #manually checked -> less reliable

all_matches <- all_matches %>%
  left_join(
    participants %>% select(reference, start_cy) %>% distinct(),
    by = c("grant" = "reference")
  )

# 5) Name cleaning 
#-------------------------------
# sometimes display_name_alternatives have longer names, we will extract the longest name.
# we will also do a bit of cleaning.
# Then We'll create a new column: name_quality
#    A = Surname has >=2 Unicode alphabetical letters AND at least one other name has >=2 Unicode alphabetical letters
#    B = Surname has >=2 Unicode alphabetical letters AND no other name >=2 Unicode alphabetical letters
#    C = Surname has <2 Unicode alphabetical letters  AND at least one other name >=2 Unicode alphabetical letters
#    D = everything else

author_work_data_name_quality <- all_matches %>%
  # Step 1: Create `cleaned_display_name`
  mutate(
    # Set to NA if forbidden characters exist
    cleaned_display_name = if_else(
      str_detect(author_name, "/|\\\\|@|twitter"),
      NA_character_,
      author_name
    ) %>%
      # Step 2: Remove leading and trailing non-Unicode alphabetical characters
      str_remove_all("^\\P{L}*") %>%              # Remove all leading non-Unicode alphabetical characters
      str_remove_all("\\P{L}*$"),                # Remove all trailing non-Unicode alphabetical characters
    # Step 3: Split `cleaned_display_name` into words
    splitted_name = str_split(cleaned_display_name, "\\s+"),
    # Step 4: Extract surname (last word in the cleaned name)
    surname = map_chr(splitted_name, ~ tail(.x, 1)),
    # Step 5: Check conditions for surname and other names
    surname_has2 = map_lgl(surname, ~ str_detect(.x, "\\p{L}{2}")), # Surname with ≥ 2 letters
    other_has2 = map_lgl(splitted_name, ~ {
      if (length(.x) <= 1) return(FALSE)         # No other names
      any(str_detect(.x[-length(.x)], "\\p{L}{2}")) # Check other names for ≥ 2 letters
    }),
    # Step 6: Assign name_quality based on conditions
    name_quality = case_when(
      is.na(cleaned_display_name) ~ "D",         # Assign "D" if cleaned_display_name is NA
      surname_has2 & other_has2  ~ "A",
      surname_has2 & !other_has2 ~ "B",
      !surname_has2 & other_has2 ~ "C",
      TRUE                       ~ "D"
    )
  ) %>%
  # Step 7: Drop intermediate columns if not needed
  select(-splitted_name, -surname, -surname_has2, -other_has2)

# only keep name quality 'A'
filtered_OA_data<-author_work_data_name_quality %>%
  filter(name_quality %in% c("A"))


# 7a) Calculate Academic Age and Filter 
# -----------------------------------------------
# Step 1: Upload Local author_id's to a Temporary Table
local_author_ids <- filtered_OA_data %>%
  select(author_id) %>%
  distinct()

dbWriteTable(con, "temp_local_authors", local_author_ids, temporary = TRUE, overwrite = TRUE, row.names = FALSE)
dbExecute(con, "CREATE INDEX idx_temp_author ON temp_local_authors(author_id)")

# Step 2: Query First Publication Year from Works Table
first_pub_year_tbl <- tbl(con, "works_authorships") %>%
  inner_join(tbl(con, "temp_local_authors"), by = "author_id") %>%
  inner_join(tbl(con, "works") %>% rename(work_id = id), by = "work_id") %>%
  group_by(author_id) %>%
  summarise(first_publication_year = min(publication_year, na.rm = TRUE))

# Step 3: Store First Publication Year as a Temporary Table
temp_first_pub_year_tbl <- first_pub_year_tbl %>%
  compute(name = "temp_first_pub_year", temporary = TRUE, overwrite = TRUE)

# Step 4: Compute Academic Age
ref_year <- min(year_range)

filtered_OA_data_academic_age <- filtered_OA_data %>%
  left_join(temp_first_pub_year_tbl %>% collect(), by = "author_id") %>%
  mutate(
    academic_age = start_cy - first_publication_year,
    academic_age_ref = ref_year - first_publication_year
  )


# 7c) Calculate Academic Age and Filter (Conditionally Apply 10-Year Gap Rule)
# -----------------------------------------------------------------------------
# Description: Calculates academic age based on a modified 'first publication year'.
#              The 10-year gap rule (finding start of last continuous segment)
#              is applied ONLY IF the author's original first publication year
#              (from step 7a) is BEFORE 1970. Otherwise, the original first
#              publication year is used.

# Assumes the following variables are already defined:
#   - 'con': A valid database connection object (e.g., DBI connection).
#   - 'local_author_ids': A dataframe/tibble with a single column 'author_id'
#                         containing the distinct author IDs you want to process.
#   - 'filtered_OA_data_academic_age': A dataframe/tibble resulting from section 7a,
#                                      containing at least columns: 'author_id',
#                                      'publication_year', 'first_publication_year',
#                                      'academic_age', 'academic_age_ref_year'.
#                                      Crucially, it MUST contain the original
#                                      'first_publication_year'.

# --- Step 1: Upload Local author_ids to a Temporary Table ---
# (Same as before)
print("Step 7c.1: Uploading author IDs to temporary table 'temp_local_authors_nogap'...")
dbWriteTable(con, "temp_local_authors_nogap", local_author_ids,
             temporary = TRUE, overwrite = TRUE, row.names = FALSE)
print("Step 7c.1: Done.")

# --- Step 2 & 3: Identify Potential Filtered First Publication Year (Using 10-Year Rule for ALL) ---
# Calculate the start of the last continuous segment for everyone.
# Store the POTENTIAL Filtered Years in a Temporary Table
print("Step 7c.2-3: Querying database to find potential start year of last continuous publication segment for all authors... \n
      Collecting potential filtered start years and storing in temporary table 'temp_potential_filtered_years'...")

first_pub_last_segment_tbl <- tbl(con, "works_authorships") %>%
  inner_join(tbl(con, "temp_local_authors_nogap"), by = "author_id") %>%
  inner_join(tbl(con, "works") %>% rename(work_id = id), by = "work_id") %>%
  filter(!is.na(publication_year)) %>%
  select(author_id, publication_year) %>%
  distinct() %>%
  group_by(author_id) %>%
  arrange(publication_year, .by_group = TRUE) %>%
  mutate(
    prev_year = lag(publication_year, order_by = publication_year)
  ) %>%
  mutate(year_gap = publication_year - prev_year) %>%
  mutate(is_new_segment_start = is.na(year_gap) | year_gap >= 10) %>%
  filter(is_new_segment_start == TRUE) %>%
  summarise(
    # This is the POTENTIAL start year based on the 10-year rule
    potential_filtered_start_year = max(publication_year, na.rm = TRUE),
    .groups = 'drop'
  ) %>%
  compute(name = "temp_potential_filtered_years", temporary = TRUE, overwrite = TRUE)

print("Step 7c.2-3: Done.")


# --- Step 4: Reference the Temporary Table ---
# (Same as before)
print("Step 7c.4: Referencing temporary table 'temp_potential_filtered_years'...")
potential_filtered_years_temp_tbl <- tbl(con, "temp_potential_filtered_years")
print("Step 7c.4: Done.")

# --- Step 5: Determine Final Filtered Year Conditionally and Compute Academic Age ---
# Join the potential filtered years, apply the condition based on original first_publication_year,
# then calculate the final academic ages.
print("Step 7c.5: Joining potential filtered start years and conditionally calculating final academic ages...")

filtered_OA_data_academic_age_nogap <- filtered_OA_data_academic_age %>%
  # Join the potentially filtered first publication years calculated in step 2
  left_join(potential_filtered_years_temp_tbl %>% collect(), by = "author_id") %>%
  
  # Determine the FINAL 'first_publication_year_filtered_nogap' based on the condition
  mutate(
    first_publication_year_filtered_nogap = case_when(
      # Apply 10-year rule result IF original first pub year is < 1970 AND the rule yielded a valid year
      first_publication_year < 1970 & !is.na(potential_filtered_start_year) ~ potential_filtered_start_year,
      
      # Otherwise (original first pub year >= 1970 OR (original < 1970 but rule yielded NA)),
      # use the original 'first_publication_year' from step 7a.
      TRUE ~ first_publication_year
    )
  ) %>%
  
  # Now calculate the academic age metrics based on the FINAL first_publication_year_filtered_nogap
  mutate(
    # Academic age relative to the specific publication year of the work
    academic_age_filtered_no_gap = start_cy - first_publication_year_filtered_nogap,
    
    # Academic age relative to a fixed reference year (e.g., 2020)
    academic_age_ref_year_filtered_no_gap = ref_year - first_publication_year_filtered_nogap
  ) %>%
  
  # Clean up the intermediate column (optional)
  select(-potential_filtered_start_year) %>%
  
  # Apply original post-processing filters and caps
  filter(!is.na(org_name)) %>% # Remove rows with missing UoA_Name
  mutate(
    # Apply caps/adjustments to the fixed-year academic age
    academic_age_ref_year_filtered_no_gap = case_when(
      is.na(academic_age_ref_year_filtered_no_gap) ~ NA_real_, # Preserve NAs from calculation
      academic_age_ref_year_filtered_no_gap < 0 ~ NA_real_,   # Negative age -> NA
      academic_age_ref_year_filtered_no_gap > 80 ~ NA_real_,  # Age > 80 -> NA
      TRUE ~ academic_age_ref_year_filtered_no_gap          # Keep valid ages
    ),
    # Apply caps/adjustments to the relative academic age
    academic_age_filtered_no_gap_2 = case_when(
      is.na(academic_age_filtered_no_gap) ~ NA_real_, # Preserve NAs
      academic_age_filtered_no_gap < 0 ~ 0,          # Negative relative age -> 0
      academic_age_filtered_no_gap > 80 ~ NA_real_, # Apply the >80 cap (as per your last code)
      TRUE ~ academic_age_filtered_no_gap
    )
  )

print("Step 7c.5: Done.")



## 8) Namsor
# ----------------------------

# --> read origin from Namsor
# --> read gender from Namsor

origin_unique <- origin %>%
  group_by(fullName) %>%
  slice_max(probabilityCalibrated, n = 1) %>%
  ungroup()

gender_unique <- gender %>%
  group_by(fullName) %>%
  slice_max(probabilityCalibrated, n = 1) %>%
  ungroup()

###Strange dash characters in some names, replace and filter a small number
complete_OA_REF_Data<-
  filtered_OA_data_academic_age_nogap %>%
  mutate(cleaned_display_name=str_replace_all(cleaned_display_name, "‐", "-"))%>%
  left_join(origin_unique %>%
              select(-c("#uid", "rowId", "countryIso2", "version", "script"))%>%
              rename(Origin_probabilityCalibrated=probabilityCalibrated,
                     Origin_probabilityCalibratedAlt=probabilityCalibratedAlt), 
            by=c("cleaned_display_name"="fullName"))%>%
  left_join(gender_unique %>%
              select(-c("#uid", "rowId", "countryIso2"))%>%
              rename(Gender_probabilityCalibrated=probabilityCalibrated,
                     Namsor_version=version,
                     Namsor_script=script),
            by=c("cleaned_display_name"="fullName"))


# 10) Process data for diversity computation ----------------------------------------------------------------------

# join the final edition of academic age scores to the main dataframe.
# the data below of Namsor-derived individual origin and gender are not 
# publicly available, and as such are included as placeholders
age_data_filtered <- filtered_OA_data_academic_age_nogap %>% distinct(author_id, academic_age_ref_year_filtered_no_gap)

complete_OA_REF_Data <- complete_OA_REF_Data %>% select(-academic_age_ref, -academic_age)

# joining needs to be done in chunks due to memory constraints

# Define the number of chunks for matching
num_chunks <- 10

# Create an index for splitting
chunk_indices <- cut(seq_len(nrow(complete_OA_REF_Data)), breaks = num_chunks, labels = FALSE)

# Split the data
data_chunks <- split(complete_OA_REF_Data, chunk_indices)

# Do the joining
joined_chunks <- lapply(data_chunks, function(chunk) {
  chunk %>% left_join(age_data_filtered)
})

complete_OA_REF_Data <- bind_rows(joined_chunks)

rm(data_chunks)
rm(joined_chunks)

# replace NA, which is Namibia, with "NB" to avoid confusion

complete_OA_REF_Data <- complete_OA_REF_Data %>% 
  mutate(countryOrigin=ifelse(is.na(countryOrigin) & !is.na(subRegion), "NB", countryOrigin))

# we want to filter the data so that only high-certainty (accurate) predictions are used
# if the origin probability is lower than 0.5, we should drop them, unless the combined
# first-and-second guess probability is >0.66, and the two countries are from the same subregion.

complete_OA_REF_Data <- left_join(complete_OA_REF_Data,
                                  complete_OA_REF_Data %>% 
                                    distinct(countryOrigin, subRegion) %>% 
                                    mutate("countryOriginAlt"=countryOrigin,
                                           "subRegionAlt"=subRegion) %>%
                                    distinct(countryOriginAlt,
                                             subRegionAlt)) # join a pseudo-dataframe so that the subregion of countryOriginAlt can be used

# filtering step based on criteria described above

complete_OA_REF_Data <- complete_OA_REF_Data %>% 
  mutate(across(
    c(region, topRegion, subRegion, countryOrigin, countryOriginAlt, subRegionAlt),
    ~ if_else(
      Origin_probabilityCalibrated > 0.5 | 
        (Origin_probabilityCalibratedAlt > 0.66 & subRegion == subRegionAlt), 
      .x, NA
    )
  ))


# now filter the gender data so that the probability >0.66; this creates 3 distinct,
# symmetrical regions: female, not sure, male.

complete_OA_REF_Data <- complete_OA_REF_Data %>% 
  mutate(likelyGender=ifelse(likelyGenderScore<0.66, NA, likelyGender))

OA_origin_gender_data <- complete_OA_REF_Data %>%
  mutate(likelyGender_ind=ifelse(likelyGender=="female", 1,
                                 ifelse(likelyGender=="male", 0, NA)))

# save the final dataframe ready for diversity computation
write_parquet(OA_origin_gender_data, "OA_origin_gender_data.parquet")


# 11) Prepare data for similarity matrix construction  ------------------------------------------------

# distance and SPI dataframe tidying for construction of similarity matrix

#Read the geographic distances and deselct the data not relevant to us

dist <- read.csv('release_2.1_2015_2019.csv') 
dist <- dist %>% filter(year=="2016") %>% distinct(iso3_o, iso3_d, distance)

# Read the SPI index (the way we format the data is the same as above)
spi <- read.csv("spi.csv")
spi <- spi %>% group_by(country_code) %>% summarise("SPI"=mean(spi, na.rm=TRUE)) %>% filter(!is.nan(SPI))

#Read the country code from geo_cepii.csv
country_code <- read.csv('country_codes.csv') %>%
  select(alpha.2, alpha.3)

colnames(country_code) <- c("iso2", "iso3")

country_code[is.na(country_code)] <- "NB" # namibia

country_code <- rbind(country_code, c("XK", "KSV")) # append kosovo

#Match dist and country_code

dist <- dist %>% left_join(country_code, by=c("iso3_o"="iso3")) %>%
  left_join(country_code, by=c("iso3_d"="iso3"))

dist <- dist %>% distinct()

# Make the SPI a distances-between-pairs df

spi_pairs <- data.frame("iso3_o"=rep(spi$country_code, each=length(spi$country_code)),
                        "iso3_d"=rep(spi$country_code, times=length(spi$country_code)),
                        "SPI_diff"=NA)

for(i in 1:nrow(spi_pairs)){
  spi_pairs[i,3] <- abs(spi[spi$country_code==spi_pairs$iso3_o[i],2] - 
                          spi[spi$country_code==spi_pairs$iso3_d[i],2])
}

spi_pairs <- spi_pairs %>% left_join(country_code, by=c("iso3_o"="iso3")) %>%
  left_join(country_code, by=c("iso3_d"="iso3"))

spi_pairs <- spi_pairs %>% distinct()

dist <- dist %>% filter(!is.na(iso2.x) & !is.na(iso2.y)) %>% 
  select(iso2.x, iso2.y, distance)

colnames(dist) <- c("country1", "country2", "distance")

spi_pairs <- spi_pairs %>% filter(!is.na(iso2.x) & !is.na(iso2.y)) %>% 
  select(iso2.x, iso2.y, SPI_diff)

colnames(spi_pairs) <- c("country1", "country2", "distance")

# save output
write.csv(dist, file="dist_clean.csv")
write.csv(spi_pairs, file="spi_clean.csv")

# calculate and export age range dataframe 
agemat <- expand.grid("age1"=seq(min(complete_OA_REF_Data$academic_age_ref_year_filtered_no_gap, na.rm=TRUE), 
                                 max(complete_OA_REF_Data$academic_age_ref_year_filtered_no_gap, na.rm=TRUE)), 
                      "age2"=seq(min(complete_OA_REF_Data$academic_age_ref_year_filtered_no_gap, na.rm=TRUE), 
                                 max(complete_OA_REF_Data$academic_age_ref_year_filtered_no_gap, na.rm=TRUE))) %>% 
  mutate("age_diff"=abs(age1-age2))

write.csv(agemat, file="age_differences.csv", row.names = FALSE)


# 12) Compute diversity indices  ----------------------------------------------------------------------

OA_origin_gender_data <- read_parquet('OA_origin_gender_data.parquet')

# load in geodesic distances 
dist <- read.csv("dist_clean.csv")[,-1]

# load in SPI differences
spi <- read.csv("spi_clean.csv")[,-1]

# load in age differences
ages <- read.csv("age_differences.csv")

# LC index code - credit and thanks to Bingzhang Chen, who produced the below file while working
# on Gok et al. (2026) https://doi.org/10.1093/reseval/rvag015
source("LC2012index.R") 

# composite functions to compute LC indices using LC2012index.R
source("diversity_computation_functions.R")

# define the communities of interest: Institution-UoA Category pairs
grouping_variables <- c("grant")

dat_communities <- OA_origin_gender_data %>% 
  drop_na(all_of(grouping_variables)) %>% 
  distinct(across(all_of(grouping_variables))) 

# remove excess data not included in the pairs
dat <- OA_origin_gender_data %>% 
  distinct(author_id, across(all_of(grouping_variables)), .keep_all = TRUE) %>% 
  semi_join(dat_communities, by = grouping_variables)  # Ensure only selected communities

# since the original data is a large object, remove it now that we have what we need from it
rm(OA_origin_gender_data)

# make a master list of all the countries for retrieval and indexing.
Countries <- Lookup_fn(dat)

# rename columns for continuity with 'dat'
colnames(Countries) <- c("countryOrigin", "subRegion", "region")

# Total number of countries and subregions and regions

TN <- TN_fn(dat)
TN_countryOrigin <- as.numeric(TN[,3])
TN_subRegion <- as.numeric(TN[,2])
TN_region <- as.numeric(TN[,1])

# make the similarity matrices corresponding to the set of countries in the 
# entire dataset. _lin denotes linear normalization, _exp denotes exponential
# with xxx denoting the value of the exponent *10^-1.
# these matrices act as 'master' matrices. when we compute the diversity scores,
# we access these matrices and draw a subset of values corresponding to the 
# matching countries.

# construct similarity matrices
dist_mat_lin <- similarity_matrix(dist, Countries$countryOrigin)$Similarity
dist_mat_exp1 <- similarity_matrix(dist, Countries$countryOrigin, 
                                   normalization_type = "exponential",
                                   normalization_exponent = 0.1)$Similarity
dist_mat_exp05 <- similarity_matrix(dist, Countries$countryOrigin, 
                                    normalization_type = "exponential",
                                    normalization_exponent = 0.05)$Similarity

# export country list for matching later with the actual countries in each
# community
dist_countries <- similarity_matrix(dist, Countries$countryOrigin)$Country_order

# do the same for SPI
spi_mat_lin <- similarity_matrix(spi, Countries$countryOrigin)$Similarity
spi_mat_exp1 <- similarity_matrix(spi, Countries$countryOrigin, 
                                  normalization_type = "exponential",
                                  normalization_exponent = 0.1)$Similarity
spi_mat_exp05 <- similarity_matrix(spi, Countries$countryOrigin, 
                                   normalization_type = "exponential",
                                   normalization_exponent = 0.05)$Similarity
spi_countries <- similarity_matrix(spi, Countries$countryOrigin)$Country_order

# do the same for ages (rename to countries for use in similarity_matrix fn) 
colnames(ages) <- c("country1", "country2", "distance")
uniqueages <- unique(dat$academic_age_ref_year_filtered_no_gap)
age_mat_lin <- similarity_matrix(ages, uniqueages)$Similarity
age_mat_exp1 <- similarity_matrix(ages, uniqueages, 
                                  normalization_type = "exponential",
                                  normalization_exponent = 0.1)$Similarity
age_mat_exp05 <- similarity_matrix(ages, uniqueages, 
                                   normalization_type = "exponential",
                                   normalization_exponent = 0.05)$Similarity
age_list <- similarity_matrix(ages, uniqueages)$Country_order


# add global reweighting values to the dataframe so that we can compute all different
# scenarios at once
dat_rw <- global_reweighting(dat)

# create a for loop so that every community has its diversity scores computed

origin_diversity <- data.frame("N_people"=rep(NA, nrow(dat_communities)), 
                               "Richness"=NA, "Gini"=NA,
                               "Exp_Shannon"=NA, "Inverse_Simpson"=NA,
                               "Scenario_1"=NA, "Scenario_2"=NA, 
                               "Scenario_3"=NA, "Scenario_4"=NA, 
                               "Scenario_5a"=NA, "Scenario_5b"=NA, 
                               "Scenario_6a"=NA, "Scenario_6b"=NA, 
                               "Scenario_7"=NA, "Scenario_8"=NA, 
                               "Scenario_9a"=NA, "Scenario_9b"=NA, 
                               "Scenario_10a"=NA, "Scenario_10b"=NA,
                               "Rao"=NA, "Rao_dist"=NA, "Rao_SPI"=NA,
                               "Prop_NA"=NA, "Prop_NA_dist"=NA, 
                               "Prop_NA_SPI"=NA)

gender_diversity <- data.frame("Female_prop"=rep(NA, nrow(dat_communities)), 
                               "Females_per_male"=NA, 
                               "Gender_Gini"=NA, "Gender_Exp_Shannon"=NA, 
                               "Gender_Inverse_Simpson"=NA, "Prop_NA_gender"=NA)

age_diversity <- data.frame("Mean_age"=rep(NA, nrow(dat_communities)), 
                            "Median_age"=NA, "SD_age"=NA, 
                            "Kurt_age"=NA, "Skew_age"=NA, 
                            "Age_Gini"=NA, "Age_Exp_Shannon"=NA, 
                            "Age_Inverse_Simpson"=NA, "Prop_NA_age"=NA, 
                            "Age_Scenario_3"=NA,
                            "Age_Scenario_5a"=NA, "Age_Scenario_5b"=NA,
                            "Age_Rao"=NA)


for(i in 1:nrow(dat_communities)){
  dat_temp <- dat_rw %>% semi_join(dat_communities[i,], by = grouping_variables)
  
  origin_diversity[i,] <- c(
    nrow(dat_temp %>% distinct(author_id)), # number of authors
    nrow(dat_temp %>% filter(!is.na(countryOrigin)) %>% distinct(countryOrigin)), # Richness
    Gini(p_country_rw(dat_temp %>% filter(!is.na(countryOrigin)))$Number), # Gini from DescTools
    ExpS(p_country_rw(dat_temp %>% filter(!is.na(countryOrigin)))$Number), # Exponential Shannon
    LC_country(dat_temp) # all other Scenarios
  )
  
  gender_diversity[i,] <- gender_diversity_fn(dat_temp$likelyGender_ind) # all gender diversity indices
  
  age_diversity[i,] <- c(
    mean(dat_temp$academic_age_filtered_no_gap_2, na.rm=TRUE), # mean academic age
    median(dat_temp$academic_age_filtered_no_gap_2, na.rm=TRUE), # median academic age
    sd(dat_temp$academic_age_filtered_no_gap_2, na.rm=TRUE), # academic age s.d.
    Kurt(dat_temp$academic_age_filtered_no_gap_2, na.rm=TRUE), # academic age kurtosis (DescTools)
    Skew(dat_temp$academic_age_filtered_no_gap_2, na.rm=TRUE), # academic age skewness (DescTools)
    age_diversity_fn(dat_temp, age_vec_name="academic_age_filtered_no_gap_2") # all diversity indices
  )
  
  if((i %% round(nrow(dat_communities)/100))==0){
    print(paste0(round(i/nrow(dat_communities)*100), "% complete at ", Sys.time())) # report progress every 1%
  }
  
  if(i>(nrow(dat_communities)-1)) {
    origin_diversity <- cbind(dat_communities, origin_diversity) # bind community data and origin diversity scores
    gender_diversity <- cbind(dat_communities, gender_diversity) # bind community data and gender diversity scores
    age_diversity <- cbind(dat_communities, age_diversity) # bind community data and age diversity scores
    rm(dat_temp) # remove excess object
  }
}

# save the results (these are the final output dataframes)
save(origin_diversity, age_diversity, 
     gender_diversity, file="OA_Diversity_results.Rdata")

OA_diversity_resuts_combined <- origin_diversity %>% full_join(gender_diversity, by=grouping_variables) %>%
  full_join(age_diversity, by=grouping_variables)

write.csv(OA_diversity_resuts_combined, "OA_diversity_combined.csv", row.names = FALSE)


# '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# Get variables for modelling
# '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# Read data
# -------------------------------
all_matches <- read_parquet("author_institution_matches_all_years_2021_2024.parquet")
all_matches <- all_matches %>% filter(match_type != "string_distance") # manually checked - not reliable

pubs <- read_rds('./project_publications_short.rds')
pubs_sub <- pubs %>% 
  filter(between(as.numeric(Year), year_range[1], year_range[2])) %>% 
  filter(ProjectReference %in% all_matches$grant)


load('./participants.Rdata')
participants <- participants %>% mutate(
  org_sector = ifelse(org_sector == "Publuc", "Public", org_sector),
  across(
    where(~ is.character(.) | is.factor(.)),
    ~ na_if(.x, "NA")
  )) %>% 
  filter(reference %in% all_matches$grant)

diversity <- read_csv("./OA_diversity_combined.csv")

OA_origin_gender_data <- read_parquet('./OA_origin_gender_data.parquet')


# Variables
# ----------------------------------

## ===== Team =======

# -> count of collaborators

# -> count of unnamed people, count of named people, total no of people, 
# -> proportion of named/total people
people_counts <- all_matches %>%
  distinct(grant, author_name) %>%
  group_by(grant) %>%
  summarise(
    n_total_authors = n_distinct(author_name),
    .groups = "drop"
  ) %>%
  
  left_join(
    pubs_sub %>%
      group_by(ProjectReference) %>%
      summarise(
        n_named_authors = n_distinct(Author),
        .groups = "drop"
      ) %>%
      rename(grant = ProjectReference),
    by = "grant"
  ) %>%
  
  mutate(
    n_named_authors = coalesce(n_named_authors, 0),
    n_unnamed_authors = pmax(n_total_authors - n_named_authors, 0),
    
    prop_named_authors = ifelse(
      n_total_authors > 0,
      pmin(n_named_authors / n_total_authors, 1),
      NA_real_
    )
  )

# -> proportion of people aged < 3,5,7,10
academic_age_prop <- all_matches %>%
  left_join(
    OA_origin_gender_data %>%
      select(grant, author_id, academic_age_filtered_no_gap_2) %>%
      distinct(),
    by = c("author_id" = "author_id", "grant" = "grant")
  ) %>%
  group_by(grant) %>%
  summarise(
    under_3_prop  = mean(academic_age_filtered_no_gap_2 < 3, na.rm = TRUE),
    under_5_prop  = mean(academic_age_filtered_no_gap_2 < 5, na.rm = TRUE),
    under_7_prop  = mean(academic_age_filtered_no_gap_2 < 7, na.rm = TRUE),
    under_10_prop = mean(academic_age_filtered_no_gap_2 < 10, na.rm = TRUE),
    .groups = "drop"
  )

# -> count of institutions
n_unique_institutions <- all_matches %>%
  group_by(grant) %>%
  summarise(
    n_unique_institutions = n_distinct(institution_id),
    .groups = "drop"
  )

# -> organisation type
org_type_counts <- all_matches %>%
  left_join(
    participants %>%
      select(reference, org_name, org_sector) %>%
      rename(grant = reference),
    by = c("grant", "org_name")
  ) %>%
  filter(!is.na(org_sector)) %>%
  group_by(grant, org_sector) %>%
  summarise(n = n_distinct(org_name), .groups = "drop") %>%  # count unique orgs per sector
  pivot_wider(
    names_from = org_sector,
    values_from = n,
    values_fill = 0,  # fill missing sectors with 0
    names_prefix = "org_type_"
  )

# -> cumulative publication count (all prior years)
cum_pub_count <- pubs %>%
  filter(Year < 2021) %>%
  group_by(ProjectReference) %>%
  summarise(
    n_cum_pub = n_distinct(Title),
    .groups = "drop"
  ) %>%
  mutate(n_cum_pub = replace_na(n_cum_pub, 0))

# -> Team UK geographic dispersion (number of distinct UK regions (NUTS2&1))
uni_group_regions <- read_csv('./University_group_regions.csv')

uni_group_regions <- uni_group_regions %>%
  mutate(
    Russell           = ifelse(grepl("Russell", Grouping), 1, 0),
    MillionPlus       = ifelse(grepl("MillionPlus", Grouping), 1, 0),
    UniversityAlliance= ifelse(grepl("UniversityAlliance", Grouping), 1, 0),
    University_Group =  as.numeric(Russell+UniversityAlliance+MillionPlus) > 0
  ) 

team_uk_region <- all_matches %>%
  left_join(
    participants %>% select(org_name, org_ukprn) %>% distinct(),
    by = "org_name"
  ) %>%
  left_join(
    uni_group_regions %>% mutate(Institution_code_UKPRN = as.character(Institution_code_UKPRN)),
    by = c("org_ukprn" = "Institution_code_UKPRN")
  ) %>%
  group_by(grant) %>%
  summarise(
    n_uk_region2 = n_distinct(Region_2),
    n_uk_region1 = n_distinct(Region_1)
  )

# -> based on affiliation: total number of countries
# total number of collaborator countries (based on affiliation)
# number of countries outside of origin
grant_country_summary <- read_csv('./grant_country_summary.csv')


## ===== Publlcation =======

# -> number of pubs
n_pubs <- pubs_sub %>%
  group_by(ProjectReference) %>%
  summarise(n_pubs = n_distinct(Title), .groups = "drop")


## ===== Grant =======

# -> primary funder dummies, secondary funder dummies, tertiary funder dummies, 
# -> programme_name, delivery_body, ukri_core_funding, start, start_cy, start_fy, 
# -> end, spend_amount, spend_category, grant_category, includes_non_university, 
# -> includes public, includes_private, includes_third, includes_only_university, 
# -> includes_non_uk_partner

# Columns of interest
cols <- c(
  "funder_l1", "funder_l2", "funder_l3",
  "programme_name", "delivery_body", "ukri_core_funding",
  "start", "start_cy", "start_fy", "end", 
  "spend_amount", "spend_category", "grant_category",
  "org_sector", "org_role", "org_country_uk_or_foreign",
  "org_ror_id", "org_ukprn", "org_pcds", "org_id"
)

# -- some checks of unique values of these cols --
# Count unique values per reference
unique_counts <- participants %>%
  group_by(reference) %>%
  summarise(across(all_of(cols), ~ n_distinct(.x, na.rm = TRUE)), .groups = "drop")

# Check which columns have more than one unique value
unique_counts %>%
  mutate(across(all_of(cols), ~ .x > 1)) %>%
  pivot_longer(
    cols = all_of(cols),
    names_to = "column",
    values_to = "has_multiple_values"
  ) %>%
  filter(has_multiple_values) %>%
  select(reference, column)
# -----------------------------------------------------------------

grant_specific_vars <- participants %>%
  
  group_by(reference) %>%
  
  summarise(
    
    # --- Keep unique fields (safe ones) ---
    across(
      c("programme_name", "delivery_body", "ukri_core_funding",
        "start", "start_cy", "start_fy", "end"),
      ~ first(.x)
    ),
    
    # --- Duration in months ---
    duration_months = time_length(interval(first(start), first(end)), "months"),
    
    # --- Spend: total + log ---
    spend_amount_total = sum(as.numeric(spend_amount), na.rm = TRUE),
    log_spend_amount_total = log1p(spend_amount_total),
    
    # --- NEW: spend per month ---
    spend_per_month = spend_amount_total / ifelse(duration_months < 1, 1, duration_months),
    
    # --- Funder dummies ---
    primary_funder = first(funder_l1),
    secondary_funder = first(funder_l2),
    tertiary_funder = first(funder_l3),
    
    # --- Co-funding count ---
    n_funders =
      (!is.na(first(funder_l1))) +
      (!is.na(first(funder_l2))) +
      (!is.na(first(funder_l3))),
    
    # --- Spend per institutions ---
    spend_per_institutions = spend_amount_total / n_distinct(org_id[!is.na(org_id)]),
    
    # --- Categories as factors ---
    spend_category = factor(first(spend_category)),
    grant_category = factor(first(grant_category)),
    
    # --- Organisation composition flags ---
    includes_non_university = any(org_sector != "University" & !is.na(org_sector)),
    
    includes_public  = any(org_sector == "Public" & !is.na(org_sector)),
    includes_private = any(org_sector == "Private" & !is.na(org_sector)),
    includes_third   = any(org_sector == "Third" & !is.na(org_sector)),
    
    includes_only_university = all(org_sector == "University" & !is.na(org_sector)),
    
    # --- Non-UK partner ---
    includes_non_uk_partner = any(org_country_uk_or_foreign == "Foreign" & !is.na(org_country_uk_or_foreign)),
    
    .groups = "drop"
  ) %>%
  
  # --- Encode funders as categorical integers ---
  mutate(
    primary_funder = as.integer(factor(primary_funder)),
    secondary_funder = as.integer(factor(secondary_funder)),
    tertiary_funder = as.integer(factor(tertiary_funder))
  )


# ===== Partner charactersitics =======
# -> No of Russell Group university, No of non-Russell Group university,
# -> No.of international named partner, count of partners
partner_summary <- participants %>%
  filter(org_role == "Partner") %>%
  left_join(
    uni_group_regions %>%
      mutate(Institution_code_UKPRN = as.character(Institution_code_UKPRN)),
    by = c("org_ukprn" = "Institution_code_UKPRN")
  ) %>%
  group_by(reference) %>%
  summarise(
    n_russell_partner = n_distinct(org_ukprn[org_sector == "University" & Russell == 1]),
    n_non_russell_partner = n_distinct(org_ukprn[org_sector == "University" & Russell == 0]),
    n_international_partner = n_distinct(org_name[org_country_uk_or_foreign == "Foreign"]),
    n_total_partners = n_distinct(org_name),
    .groups = "drop"
  )


## ===== Interdisciplinarity =======
multidisciplinary <- read_csv('./multidisciplinary_index_final_results/all_scenarios.csv')

grant_multidisciplinary_index_scenario_summary <- all_matches %>%
  select(grant, work_id) %>%
  distinct() %>%
  left_join(
    multidisciplinary %>%
      mutate(PublicationId = paste0("https://openalex.org/W", PublicationId)),
    by = c("work_id" = "PublicationId")
  ) %>%
  pivot_longer(
    cols = starts_with("Scenario_"),
    names_to = "scenario",
    values_to = "value"
  ) %>%
  group_by(grant, scenario) %>%
  summarise(
    mean   = mean(value, na.rm = TRUE),
    median = median(value, na.rm = TRUE),
    sd     = sd(value, na.rm = TRUE),
    min    = min(value, na.rm = TRUE),
    max    = max(value, na.rm = TRUE),
    
    prop_50 = mean(value > 0.5, na.rm = TRUE),
    prop_75 = mean(value > 0.75, na.rm = TRUE),
    prop_90 = mean(value > 0.90, na.rm = TRUE),
    prop_95 = mean(value > 0.95, na.rm = TRUE),
    prop_99 = mean(value > 0.99, na.rm = TRUE),
    
    n_pubs = n_distinct(work_id),
    
    shannon = diversity(value, index = "shannon"),
    simpson = diversity(value, index = "simpson"),
    gini    = Gini(value),
    
    .groups = "drop"
  )

grant_multidisciplinary_index_summary_wide <- grant_multidisciplinary_index_scenario_summary %>%
  pivot_longer(
    cols = c(mean, median, sd, min, max, prop_50, prop_75, prop_90, prop_95, prop_99, n_pubs, shannon, simpson, gini),
    names_to = "stat",
    values_to = "val"
  ) %>%
  unite("scenario_stat", scenario, stat) %>%
  pivot_wider(
    names_from = scenario_stat,
    values_from = val
  ) %>%
  rename_with(~ paste0("interdisciplinary_", .x), -grant)


#''''''''''''''''''''''''''''''''''''''
# Get collaborator countries
#''''''''''''''''''''''''''''''''''''''
INPUT_FILE  <- "author_institution_matches_all_years_2021_2024.parquet"
OUTPUT_FILE <- "doi_author_countries.parquet"

# ---- Load Input ----
OA_origin_gender_data=read_parquet('./OA_origin_gender_data.parquet')

all_matches <- read_parquet(INPUT_FILE)
unique_dois  <- all_matches %>%
  filter(!is.na(doi_clean)) %>%
  distinct(doi_clean)

message("[INFO] Unique DOIs to process: ", nrow(unique_dois))

# ---- DB Connection ----
source('connect_OA_DB.R')

on.exit({
  message("\n[CLEANUP] Closing DB connection...")
  dbDisconnect(con)
  message("[CLEANUP] Done.")
}, add = TRUE)

# ---- Upload DOIs to Temp Table ----
message("[STEP 1] Uploading ", nrow(unique_dois), " DOIs to temp table...")

dbWriteTable(
  con, "temp_dois", unique_dois,
  temporary = TRUE, overwrite = TRUE, row.names = FALSE
)
dbExecute(con, "CREATE INDEX idx_temp_dois ON temp_dois(doi_clean);")
dbExecute(con, "ANALYZE temp_dois;")

message("[STEP 1] Done.")


# ---- Query: DOI -> work_id -> author_id -> institution -> country_code ----
#
# OpenAlex schema (openalex schema):
#   works                        : id (work_id), doi
#   works_authorships            : work_id, author_id, institution_id
#   institutions                 : id (institution_id), display_name, country_code
#
# One authorship can have multiple institutions, so we keep all rows.
# If an author has no institution row the country will be NA.

# ---- Pre-materialise works with cleaned DOI (indexed) ----
message("[STEP 2a] Materialising filtered works on server...")


for (yr in year_range) {
  
  message(sprintf("[STEP 2a] Creating temp table for %d...", yr))
  
  # 1. Create temp table of works with DOIs for the year
  dbExecute(con, sprintf("
    CREATE TEMP TABLE temp_works_doi_%d AS
    SELECT w.id AS work_id,
           w.publication_year
    FROM openalex.works w
    WHERE w.doi IS NOT NULL
      AND w.publication_year = %d
      AND LOWER(REPLACE(w.doi, 'https://doi.org/', '')) IN (
          SELECT doi_clean FROM temp_dois
      );
  ", yr, yr))
  
  # 2. Index for faster joins
  dbExecute(con, sprintf("CREATE INDEX idON_temp_works_doi_%d ON temp_works_doi_%d(work_id);", yr, yr))
  message(sprintf("[STEP 2a] Done for %d.", yr))
  
  # 3. Reference temp table in dplyr
  temp_workdoi <- tbl(con, sprintf("temp_works_doi_%d", yr))
  
  # 4. Reference works_authorships and institutions tables
  works_authorships_tbl <- tbl(con, in_schema("openalex", "works_authorships")) %>%
    filter(!is.na(institution_id)) %>%
    select(work_id, author_id, institution_id)
  
  institutions_tbl <- tbl(con, in_schema("openalex", "institutions")) %>%
    select(institution_id = id, country_code)
  
  # 5. Join and compute temporary table
  message(sprintf("[STEP 2b] Running main join for %d...", yr))
  
  work_author_inst_temp <- temp_workdoi %>%
    inner_join(works_authorships_tbl, by = "work_id") %>%
    left_join(institutions_tbl, by = "institution_id") %>%
    compute(
      name = sprintf("temp_work_author_inst_%d", yr),
      temporary = TRUE,
      overwrite = TRUE
    )
  
  message(sprintf("[STEP 2b] Done for %d.", yr))
}


# Map works → grants from OA dataframe
grant_work_tbl <- OA_origin_gender_data %>%
  select(grant, work_id) %>%
  distinct() %>%
  copy_to(con, ., "grant_work_tbl", temporary = TRUE, overwrite = TRUE)

# Copy OA work-author mapping to server
oa_work_author_tbl <- OA_origin_gender_data %>%
  select(work_id, author_id) %>%
  distinct() %>%
  copy_to(con, ., "oa_work_author_tbl", temporary = TRUE, overwrite = TRUE)

# creating index
dbExecute(con, "CREATE INDEX idx_oa_work_author ON oa_work_author_tbl(work_id, author_id);")
dbExecute(con, "CREATE INDEX idx_grant_work ON grant_work_tbl(work_id);")
for (yr in year_range) {
  tbl_name <- sprintf("temp_work_author_inst_%d", yr)
  dbExecute(con, sprintf(
    "CREATE INDEX idx_%s_work_author ON %s(work_id, author_id);",
    tbl_name, tbl_name
  ))
}

# Combine temp tables fully in SQL
all_temp_tbl <- year_range %>%
  map(~ tbl(con, sprintf("temp_work_author_inst_%d", .x))) %>%
  reduce(union_all)


# team institutions and countries
grant_team_inst_country <- all_temp_tbl %>%
  inner_join(tbl(con, "oa_work_author_tbl"), by = c("work_id", "author_id")) %>%
  right_join(tbl(con, "grant_work_tbl"), by = "work_id") %>%
  select(grant, institution_id, country_code) %>%
  distinct() %>%
  compute(name = "grant_team_inst_countries", temporary = TRUE, overwrite = TRUE) %>%  
  distinct() %>%
  collect()

# team + collaborators institutions and countries
all_inst_country <- all_temp_tbl %>%
  inner_join(tbl(con, "grant_work_tbl"), by = "work_id") %>%  # assign grants
  select(grant, institution_id, country_code) %>%
  distinct() %>%
  compute(name = "grant_all_inst_countries", temporary = TRUE, overwrite = TRUE) %>%  
  distinct() %>%
  collect()


# Team countries per grant
n_team_countries <- grant_team_inst_country %>%
  group_by(grant) %>%
  summarise(
    unique_countries_team = list(sort(unique(country_code))),
    n_team_countries = n_distinct(country_code),
    .groups = "drop"
  )

# collaborators countries per grant
n_collaborators_countries <- all_inst_country %>%
  anti_join(grant_team_inst_country, by = c("grant", "institution_id")) %>%
  group_by(grant) %>%
  summarise(
    n_collaborators = n_distinct(institution_id),
    unique_countries_collab = list(sort(unique(country_code))),
    n_collaborators_countries = n_distinct(country_code),
    .groups = "drop"
  )

# ---- number of countries outside of origin ----
n_countries_df <- n_team_countries %>%
  full_join(n_collaborators_countries, by = "grant") %>%
  mutate(
    unique_countries_team = map(unique_countries_team, ~ .x %||% character(0)),
    unique_countries_collab = map(unique_countries_collab, ~ .x %||% character(0))
  ) %>%
  mutate(
    n_countries_outside_origin = map2_int(
      unique_countries_collab,
      unique_countries_team,
      ~ length(setdiff(.x, .y))
    )
  ) %>%
  mutate(
    n_collaborators = replace_na(n_collaborators, 0),
    n_collaborators_countries = replace_na(n_collaborators_countries, 0)
  )

write_csv(n_countries_df, './grant_country_summary.csv')

# --------------------------
# -----  Combine ----------
# --------------------------
full_data <- diversity %>% 
  left_join(people_counts, by = "grant") %>%
  left_join(academic_age_prop, by = "grant") %>%
  left_join(n_unique_institutions, by = "grant") %>%
  left_join(org_type_counts, by = "grant") %>%
  left_join(cum_pub_count, by = c("grant"="ProjectReference")) %>%
  left_join(team_uk_region, by = "grant") %>%
  left_join(grant_country_summary, by = "grant") %>%
  left_join(n_pubs, by = c("grant"="ProjectReference")) %>%
  left_join(grant_specific_vars, by = c("grant"="reference")) %>%
  left_join(partner_summary, by = c("grant"="reference")) %>%
  left_join(grant_multidisciplinary_index_summary_wide, by = "grant")

full_data <- full_data %>% filter(between(start_cy, 2021, 2024)) # include projects between 2021 and 2024

write.csv(full_data, "diversity_outcomes_and_characteristics.csv", row.names = FALSE)


#''''''''''''''''''''''''''''''''''''''
# Get PI information
#''''''''''''''''''''''''''''''''''''''
# ---- Load Input ----
OA_origin_gender_data=read_parquet('./OA_origin_gender_data.parquet')

all_matches <- read_parquet(INPUT_FILE)

people <- read_csv('./gtr+_projects_people.csv')

# ---- DB Connection ----
source('connect_OA_DB.R')

on.exit({
  message("\n[CLEANUP] Closing DB connection...")
  dbDisconnect(con)
  message("[CLEANUP] Done.")
}, add = TRUE)


titles <- c("Dr\\.?", "Professor", "Sir", "Madam", "Dr\\. Md\\.", "Mr\\.?", "Mrs\\.?", "Ms\\.?", "Miss") # all Titles
titles_regex <- paste0("^(", paste(titles, collapse = "|"), ")\\b") # for matching Titles

people_cleaned_na_oa_id <- people %>%
  filter(role == "PI_PER", is.na(oa_id), reference %in% all_matches$grant) %>%           # only PIs with missing OAID
  distinct(first_name, surname, .keep_all = TRUE) %>%
  mutate(
    across(
      c(first_name, surname),
      ~ .x %>%
        str_replace_all("[\\\\/,]", " ") %>%          # replace / or \ with space
        str_remove(titles_regex),                  # remove Title at start of the first name
      .names = "cleaned_{.col}"
    ),
    
    # Extract all (middle_name) anywhere in first_name or surname
    first_middle   = str_extract(cleaned_first_name, "\\(([^)]+)\\)") %>% str_remove_all("[()]"),
    surname_middle = str_extract(cleaned_surname, "\\(([^)]+)\\)") %>% str_remove_all("[()]"),
    
    # Remove (middle_name) from both columns
    cleaned_first_name = str_trim(str_remove_all(cleaned_first_name, "\\([^)]*\\)")),
    cleaned_surname   = str_trim(str_remove_all(cleaned_surname, "\\([^)]*\\)")),
    
    # Check if each cleaned column has ≥2 consecutive letters
    surname_has2 = map_lgl(cleaned_surname, ~ !is.na(.x) & str_detect(.x, "\\p{L}{2}")),
    other_has2   = map_lgl(cleaned_first_name, ~ !is.na(.x) & str_detect(.x, "\\p{L}{2}")), 
    
    # Remove all leading/trailing non-letter characters and capitalize first letter of each word/hyphen
    across(
      c(cleaned_first_name, cleaned_surname),
      ~ .x %>%
        str_replace_all("^\\P{L}+|\\P{L}+$", "") %>%                       # trim non-letters at ends
        str_replace_all("(^|\\-|\\s)(\\p{L})", function(m) toupper(m)) %>% # uppercase after space or hyphen
        str_replace_all("(?<=\\p{L})(\\p{L})", function(m) tolower(m)) %>% # lowercase remaining letters
        str_squish()                                                       # collapse multiple spaces to one
    )) %>%
  select(-first_middle, -surname_middle, -surname_has2, -other_has2)

# Get PIs OpenAlex IDs for the ones without OpenAlex IDs
matched_na_oa_id_pis <- people_cleaned_na_oa_id %>% 
  mutate(fullname = str_to_lower(str_squish(paste(first_name, surname)))) %>% 
  left_join(
    all_matches %>%
      select(author_name, author_id) %>%
      distinct() %>%
      mutate(author_name = str_to_lower(str_squish(author_name))),
    by = c("fullname" = "author_name")
  ) 

# Fill matched PIs with missing OA ID with OpenAlex IDs
people_updated_oa_id <- people %>%
  filter(role == "PI_PER", reference %in% all_matches$grant) %>%
  mutate(oa_id = if_else(!is.na(oa_id),
                         paste0("https://openalex.org/", oa_id),
                         oa_id)) %>%
  left_join(
    matched_na_oa_id_pis %>% select(reference, first_name, surname, author_id),
    by = c("reference", "first_name", "surname")
  ) %>%
  mutate(oa_id = if_else(is.na(oa_id), author_id, oa_id)) %>%
  select(-author_id)

unique_author_ids  <- people_updated_oa_id %>%
  filter(!is.na(oa_id)) %>%
  distinct(oa_id)

dbWriteTable(
  con, "temp_author_ids", unique_author_ids,
  temporary = TRUE, overwrite = TRUE, row.names = FALSE
)
dbExecute(con, "CREATE INDEX idx_temp_author_ids ON temp_author_ids(oa_id);")
dbExecute(con, "ANALYZE temp_author_ids;")

# -> PI cumulative publication count (all prior years)
works_count_pre_cutoff <- dbGetQuery(
  con,
  sprintf("
    SELECT t.oa_id,
           SUM(ay.works_count) AS total_works_pre_cutoff
    FROM temp_author_ids t
    JOIN openalex.authors_counts_by_year ay
      ON t.oa_id = ay.author_id
    WHERE ay.year < %d
    GROUP BY t.oa_id;
  ", min(year_range))
)

# -> PI number of prior UKRI grants (all prior years)
# 1️. Earliest start from selected grants
earliest_grant_pre_cutoff <- people_updated_oa_id %>%
  filter(!is.na(oa_id), reference %in% full_data$grant) %>%
  left_join(
    participants %>% select(reference, start) %>% distinct(),
    by = "reference"
  ) %>%
  group_by(oa_id) %>%
  summarise(
    earliest_start_pre_cutoff = if (all(is.na(start))) NA else min(start, na.rm = TRUE),
    .groups = "drop"
  )

# 2. Count all previous grants
grants_before <- people_updated_oa_id %>%
  filter(!is.na(oa_id)) %>%
  left_join(
    participants %>% select(reference, start) %>% distinct(),
    by = "reference"
  ) %>%
  distinct(oa_id, reference, start) %>%   # ensure unique grants
  left_join(earliest_grant_pre_cutoff, by = "oa_id") %>%
  group_by(oa_id) %>%
  summarise(
    grants_before_earliest = sum(start < earliest_start_pre_cutoff, na.rm = TRUE),
    is_first_PI_grant = if (all(is.na(grants_before_earliest))) {
      NA_integer_
    } else {
      as.integer(any(!is.na(grants_before_earliest)) && min(grants_before_earliest, na.rm = TRUE) == 0)
    },
    .groups = "drop"
  )


# -> PI Academic Age
pi_academic_age <- people_updated_oa_id %>%
  left_join(
    OA_origin_gender_data %>%
      select(author_id, academic_age_ref_year_filtered_no_gap) %>% distinct(),
    by = c("oa_id" = "author_id")
  ) %>%
  select(oa_id, academic_age_ref_year_filtered_no_gap)

# -> is first grant as PI or coI
earliest_grant_coi <- people %>%
  filter(role %in% c("COI_PER", "RESEARCH_COI_PER"), !is.na(oa_id), reference %in% full_data$grant) %>%
  mutate(oa_id = paste0("https://openalex.org/", oa_id)) %>%   # prepend OpenAlex URL
  left_join(
    participants %>% select(reference, start) %>% distinct(),
    by = "reference"
  ) %>%
  group_by(oa_id) %>%
  summarise(
    earliest_start_coi = if (all(is.na(start))) NA else min(start, na.rm = TRUE),
    .groups = "drop"
  )

grants_before_coi <- people %>%
  filter(role %in% c("COI_PER", "RESEARCH_COI_PER"), !is.na(oa_id)) %>%
  mutate(oa_id = paste0("https://openalex.org/", oa_id)) %>%   # prepend OpenAlex URL
  left_join(
    participants %>% select(reference, start) %>% distinct(),
    by = "reference"
  ) %>%
  distinct(oa_id, reference, start) %>%   # ensure unique grants
  left_join(earliest_grant_coi, by = "oa_id") %>%
  group_by(oa_id) %>%
  summarise(
    grants_before_earliest = sum(start < earliest_start_coi, na.rm = TRUE),
    is_first_grant_as_coi = if (all(is.na(grants_before_earliest))) {
      NA_integer_
    } else {
      as.integer(any(!is.na(grants_before_earliest)) && min(grants_before_earliest, na.rm = TRUE) == 0)
    },
    .groups = "drop"
  )

# Combine PI and Co-I first grant
first_grants_combined <- grants_before %>%   # from PI pipeline
  select(oa_id, is_first_PI_grant) %>%
  full_join(
    grants_before_coi %>% select(oa_id, is_first_grant_as_coi),   # from Co-I pipeline
    by = "oa_id"
  ) %>%
  mutate(
    is_first_grant_of_PI_or_COI = case_when(
      !is.na(is_first_PI_grant) & is_first_PI_grant == 1 ~ 1L,
      !is.na(is_first_grant_as_coi) & is_first_grant_as_coi == 1 ~ 1L,
      TRUE ~ 0L
    )
  ) %>%
  select(-is_first_grant_as_coi)

pi_summary <- pi_academic_age %>%
  full_join(works_count_pre_cutoff, by = "oa_id") %>%
  full_join(first_grants_combined, by = "oa_id") %>%
  full_join(grants_before %>% select(oa_id, grants_before_earliest), by = "oa_id")

pi_summary <- pi_summary %>%
  rename(
    pi_cumulative_publication_count = total_works_pre_cutoff,
    pi_academic_age_2021 = academic_age_ref_year_filtered_no_gap,
    PI_n_prior_grants = grants_before_earliest
  )

write.csv(pi_summary, "PI_characteristics.csv", row.names = FALSE)
