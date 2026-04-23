library(tidyverse) 
library(dbplyr)
library(jsonlite)
library(httr2)
library(readxl)
library(DBI)
library(data.table)
library(VennDiagram)
library(stringdist)


# venn.plot <- draw.pairwise.venn(
#   area1 = length(unique(participants_with_RC$reference)), # total in people
#   area2 = length(unique(publication$ProjectReference)), # total in publication
#   cross.area = length(intersect(
#     unique(participants_with_RC$reference),
#     unique(publication$ProjectReference)
#   )), # in both
#   category = c("Participants", "Publication"),
#   fill = c("skyblue", "pink"))


# ------------------------------------------------------------
# Given a DOI and a subset of publications (df_subset)
# 1. find the publication using DOI from OpenAlex API
# 2. Extracts all authors and their institutions from the API response
# 3. find authors with institutions listed in gtr+ publication dataset

#-------------------------------------------------------------

final_RC_publications_short <- read_rds('./project_publications_short.rds')

# batch API call, exact match and fuzzy match for non-ROR
get_authors_batch <- function(dois, df_all, fuzzy_threshold = 0.2) {
  # Trim DOIs
  dois <- str_trim(dois)
  
  # Split into batches of 50 DOIs
  batches <- split(dois, ceiling(seq_along(dois) / 50))
  
  all_results <- map_dfr(batches, function(batch) {
    # Encode DOIs for URL safety
    batch_enc <- URLencode(batch)
    
    # Use OR filter (pipe |) for batch query
    doi_filter <- paste0("doi:", paste(batch_enc, collapse = "|"))
    
    # Construct URL
    url <- paste0(
      "https://api.openalex.org/works?filter=",
      doi_filter,
      "&per-page=50"
    )
    
    # API request
    resp <- request(url) %>%
      req_headers(Accept = "application/json") %>%
      req_perform() %>%
      resp_body_json(simplifyVector = FALSE)
    
    # Results is a list of works
    works <- resp$results
    
    # Map over works (keep your original logic)
    map_dfr(works, function(work) {
      doi <- work$doi
      df_subset <- df_all %>% filter(str_trim(DOI) == str_trim(doi))
      
      authors <- work$authorships
      if (length(authors) == 0) return(NULL)
      
      map_dfr(authors, function(a) {
        insts <- a$institutions
        if (length(insts) == 0) return(NULL)
        
        inst_names <- map_chr(insts, ~ .x$display_name)
        inst_rors <- map_chr(insts, ~ .x$ror %||% NA_character_)
        inst_ids <- map_chr(insts, ~ .x$id)
        inst_countries <- map_chr(insts, ~ .x$country_code %||% NA_character_)
        
        df_na_ror <- df_subset %>% filter(is.na(org_ror_id))
        df_with_ror <- df_subset %>% filter(!is.na(org_ror_id))
        
        # ---> uncomment this for fuzzy match for non-ROR institutes
        #
        # Vectorized match
        match_ror <- !is.na(inst_rors) & inst_rors %in% df_with_ror$org_ror_id
        match_fuzzy <- rep(FALSE, length(inst_names))
        
        if (!any(match_ror) && nrow(df_na_ror) > 0) {
          match_fuzzy <- map_lgl(seq_along(inst_names), function(i) {
            x <- tolower(inst_names[i])
            x_country <- inst_countries[i]
            any(
              (stringdist::stringdist(x, tolower(df_na_ror$org_name), method = "jw") < fuzzy_threshold |
                 str_detect(x, paste0("\\b", tolower(df_na_ror$org_name), "\\b"))) &
                df_na_ror$org_country_code == x_country
            )
          })
        }
        
        keep <- if (any(match_ror)) match_ror else match_fuzzy
        if (!any(keep)) return(NULL)
        
        # ---> comment this if use fuzzy match  =========
        if (!any(match_ror)) return(NULL)
        
        tibble(
          ProjectReference = df_subset$ProjectReference[1],
          DOI              = doi,
          Author           = a$author$display_name,
          Author_ID        = a$author$id,
          Author_ORCID     = a$author$orcid %||% NA_character_,
          Institutions     = paste(inst_names[keep], collapse = "; "),
          Inst_RORs        = paste(inst_rors[keep], collapse = "; "),
          Inst_IDs         = paste(inst_ids[keep], collapse = "; "),
          Work_ID          = work$id,
          Year             = work$publication_year
        )
      })
    })
  })
  
  return(all_results)
}


doi_all <- final_RC_publications_short %>%
  filter(DOI != "", Year == 2019) %>%
  pull(DOI) %>%
  unique() %>%
  str_trim()

# split into chunks of chunk size
chunk_size <- 500
doi_chunks <- split(doi_all, ceiling(seq_along(doi_all) / chunk_size))

authors_chunk <- get_authors_batch_ror_fast(
  doi_chunks[[i]],
  final_RC_publications_short
)

