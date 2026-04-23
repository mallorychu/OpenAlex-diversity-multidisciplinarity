import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

# ==============================
# 1. CONNECT TO DATABASE
# ==============================
load_dotenv()

db_url = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

engine = create_engine(db_url)

with engine.begin() as conn:
    conn.execute(text("SET work_mem='1GB'"))
    conn.execute(text("SET maintenance_work_mem='1GB'"))

print("Connected to database.")

# ==============================
# 2. DEFINE YEAR RANGE
# ==============================
start_year = 2021
end_year = 2024

# ==============================
# 3. LOAD PUBLICATIONS
# ==============================
all_publication = pd.read_parquet("../author_institution_matches_all_years_2021_2024.parquet")
all_publication = all_publication.rename(columns={"year": "publication_year"})
all_publication = all_publication[
    (all_publication.publication_year >= start_year) &
    (all_publication.publication_year <= end_year)
]
print("Publications loaded:", len(all_publication))

# ==============================
# 4. CREATE TEMP TABLE FOR WORK IDS
# ==============================
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TEMP TABLE temp_work_ids (
            work_id TEXT PRIMARY KEY
        );
    """))

# ==============================
# 5. COPY PUBLICATION WORK IDS INTO TEMP TABLE
# ==============================
buffer = StringIO()
all_publication["work_id"].drop_duplicates().to_csv(buffer, index=False, header=False)
buffer.seek(0)

raw_conn = engine.raw_connection()
cursor = raw_conn.cursor()
cursor.copy_expert("COPY temp_work_ids(work_id) FROM STDIN WITH CSV", buffer)
raw_conn.commit()
cursor.close()
raw_conn.close()
print("Publication work IDs loaded.")

# ==============================
# 6. INDEX TEMP TABLE
# ==============================
with engine.begin() as conn:
    conn.execute(text("CREATE INDEX idx_temp_work_ids ON temp_work_ids(work_id);"))

# ==============================
# 7. FETCH REFERENCES (pub2ref)
# ==============================
pub2ref_sql = """
SELECT
    wr.work_id,
    wr.referenced_work_id
FROM openalex.works_referenced_works wr
JOIN temp_work_ids t ON wr.work_id = t.work_id
"""
pub2ref_df = pd.read_sql(pub2ref_sql, engine)
pub2ref_df = pub2ref_df.merge(
    all_publication[["work_id", "publication_year"]],
    on="work_id",
    how="left"
)

# ==============================
# 8. SAVE pub2ref
# ==============================
pub2ref_file = f"pub2ref_{start_year}_{end_year}.parquet"
pub2ref_df.to_parquet(pub2ref_file, compression="zstd", index=False)
print("Saved pub2ref:", pub2ref_file)

# ==============================
# 9. CREATE TEMP TABLE FOR REFERENCED WORK IDS
# ==============================
ref_ids = pub2ref_df["referenced_work_id"].dropna().unique()
ref_df = pd.DataFrame({"work_id": ref_ids})

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TEMP TABLE temp_ref_ids (
            work_id TEXT PRIMARY KEY
        );
    """))

buffer = StringIO()
ref_df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

raw_conn = engine.raw_connection()
cursor = raw_conn.cursor()
cursor.copy_expert("COPY temp_ref_ids(work_id) FROM STDIN WITH CSV", buffer)
raw_conn.commit()
cursor.close()
raw_conn.close()
print("Referenced work IDs loaded.")

# ==============================
# 10. INDEX TEMP TABLE FOR REFERENCED WORKS
# ==============================
with engine.begin() as conn:
    conn.execute(text("CREATE INDEX idx_temp_ref_ids ON temp_ref_ids(work_id);"))

# ==============================
# 11. MATERIALIZE PUBLICATION TOPICS
# ==============================
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TEMP TABLE pub_topics AS
        SELECT *
        FROM (
            SELECT
                wt.work_id,
                wt.topic_id,
                wt.score,
                ROW_NUMBER() OVER (
                    PARTITION BY wt.work_id
                    ORDER BY wt.score DESC
                ) AS rn
            FROM openalex.works_topics wt
            JOIN temp_work_ids t ON wt.work_id = t.work_id
            WHERE wt.score >= 0.8
        ) ranked
        WHERE rn <= 5;
    """))

pub_topics_df = pd.read_sql("SELECT * FROM pub_topics", engine)
pub_topics_df["type"] = "publication"
pub_topics_df = pub_topics_df.merge(
    all_publication[["work_id", "publication_year"]],
    on="work_id",
    how="left"
)
print("Publication topics fetched.")

# ==============================
# 12. MATERIALIZE REFERENCED TOPICS
# ==============================
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TEMP TABLE ref_topics AS
        WITH ref_works AS (
            SELECT w.id AS work_id, w.publication_year
            FROM openalex.works w
            JOIN temp_ref_ids r ON w.id = r.work_id
        ),
        ranked_topics AS (
            SELECT
                wt.work_id,
                wt.topic_id,
                wt.score,
                rw.publication_year,
                ROW_NUMBER() OVER (
                    PARTITION BY wt.work_id
                    ORDER BY wt.score DESC
                ) AS rn
            FROM openalex.works_topics wt
            JOIN ref_works rw ON wt.work_id = rw.work_id
            WHERE wt.score >= 0.8
        )
        SELECT * FROM ranked_topics WHERE rn <= 5;
    """))

ref_topics_df = pd.read_sql("SELECT * FROM ref_topics", engine)
ref_topics_df["type"] = "referenced"
print("Referenced topics fetched.")

# ==============================
# 13. COMBINE TOPICS
# ==============================
full_topics_df = pd.concat([pub_topics_df, ref_topics_df], ignore_index=True)
print("Total topic rows:", len(full_topics_df))

# ==============================
# 14. FETCH TOPIC METADATA FROM OPENALEX API
# ==============================
def fetch_openalex_topics(topic_ids, batch_size=50, max_workers=5, debug=False):
    topic_ids = [tid if tid.startswith("https://openalex.org/") else f"https://openalex.org/{tid}" for tid in topic_ids]
    batches = [topic_ids[i:i+batch_size] for i in range(0, len(topic_ids), batch_size)]
    all_results = []

    def _fetch_batch(batch):
        ids = "|".join(batch)
        url = f"https://api.openalex.org/topics?filter=id:{ids}&select=id,display_name,field,subfield"
        if debug:
            print(f"\nFetching batch: {batch}")
            print("URL:", url)

        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        flat = []
        for t in results:
            flat.append({
                "topic_id": t.get("id"),
                "topic_name": t.get("display_name"),
                "field_id": t.get("field", {}).get("id"),
                "field_name": t.get("field", {}).get("display_name"),
                "subfield_id": t.get("subfield", {}).get("id"),
                "subfield_name": t.get("subfield", {}).get("display_name")
            })
        return flat

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_batch, b) for b in batches]
        for f in as_completed(futures):
            all_results.extend(f.result())
            sleep(0.5)  # polite pause

    return pd.DataFrame(all_results)

print("Fetching topic metadata...")
topic_meta_df = fetch_openalex_topics(full_topics_df["topic_id"].unique().tolist())
full_topics_df = full_topics_df.merge(topic_meta_df, on="topic_id", how="left")

# ==============================
# 15. SAVE FINAL COMBINED TOPICS
# ==============================
output_file = f"full_topics_snapshot_{start_year}_{end_year}.parquet"
full_topics_df.to_parquet(output_file, compression="zstd", index=False)
print("Saved full topics:", output_file)


## ''''''''''''''''''''''''''''''''''
## index computation
## ''''''''''''''''''''''''''''''''''
import os
import logging
import time
import gc
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyscisci.methods.raostirling import (
    field_citation_distance,
    raostriling_interdisciplinarity,
)
from math import ceil
from collections import defaultdict

# =============================================================================
# CONFIGURATION
# =============================================================================
PUB2REF_PARQUET = "./pub2ref_2021_2024.parquet"
TOPICS_PARQUET = "./full_topics_snapshot_2021_2024.parquet"

OUTPUT_DIR = "raostirling_intermediate_results"
DIST_DIR = "raostirling_distance_matrix"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DIST_DIR, exist_ok=True)

BATCH_SIZE = 5000

RUN_ALL_SCENARIOS = False
PARTIAL_SCENARIO = 1

SCENARIOS = [
    {"Scenario": 1,  "metric": "cosine",    "temporal": True,  "level": 1},
    {"Scenario": 2,  "metric": "euclidean", "temporal": True,  "level": 1},
    {"Scenario": 3,  "metric": "cosine",    "temporal": False, "level": 1},
    {"Scenario": 4,  "metric": "euclidean", "temporal": False, "level": 1},
    {"Scenario": 5,  "metric": "cosine",    "temporal": True,  "level": 2},
    {"Scenario": 6,  "metric": "euclidean", "temporal": True,  "level": 2},
    {"Scenario": 7,  "metric": "cosine",    "temporal": False, "level": 2},
    {"Scenario": 8,  "metric": "euclidean", "temporal": False, "level": 2},
    {"Scenario": 9,  "metric": "cosine",    "temporal": True,  "level": 3},
    {"Scenario": 10, "metric": "euclidean", "temporal": True,  "level": 3},
    {"Scenario": 11, "metric": "cosine",    "temporal": False, "level": 3},
    {"Scenario": 12, "metric": "euclidean", "temporal": False, "level": 3},
]

TOPIC_LEVEL_MAP = {1: "topic_id", 2: "subfield_id", 3: "field_id"}

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("rao")

# =============================================================================
# HELPERS
# =============================================================================
def extract_openalex_id_numeric(df, columns):
    if isinstance(columns, str):
        columns = [columns]
    for col in columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.split("/")
            .str[-1]
            .str[1:]
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def downcast_numeric_df(df):
    for col in df.select_dtypes(include=["number"]).columns:
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast="integer")
        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast="float")
    return df


def enforce_category(df):
    for c in df.select_dtypes(include="str").columns:
        df[c] = df[c].astype("category")
    return df


def strip_categories(df):
    """Convert categorical columns back to their base dtype.
    pyscisci needs plain int/str for FieldId and PublicationId."""
    for col in df.columns:
        if isinstance(df[col].dtype, pd.CategoricalDtype):
            # Convert back to the original base dtype
            df[col] = df[col].astype(df[col].cat.categories.dtype)
    return df


# =============================================================================
# LOAD INPUTS
# =============================================================================
def load_inputs():
    log.info("Reading parquet files...")
    pub2ref = pd.read_parquet(PUB2REF_PARQUET)
    topics = pd.read_parquet(TOPICS_PARQUET)
    
    pub2ref = pub2ref.drop_duplicates()
    topics = topics.drop_duplicates()

    pub2ref = extract_openalex_id_numeric(pub2ref, ["work_id", "referenced_work_id"])
    topics = extract_openalex_id_numeric(
        topics, ["work_id", "topic_id", "subfield_id", "field_id"]
    )

    pub2ref = downcast_numeric_df(pub2ref)
    topics = downcast_numeric_df(topics)

    pub2ref = enforce_category(pub2ref)
    topics = enforce_category(topics)

    # Rename for pyscisci compatibility
    col_map = {}
    for c in pub2ref.columns:
        lc = c.lower()
        if lc == "work_id":
            col_map[c] = "CitingPublicationId"
        elif "referenced" in lc or "cited" in lc:
            col_map[c] = "CitedPublicationId"
        elif "year" in lc:
            col_map[c] = "CitingYear"
    pub2ref = pub2ref.rename(columns=col_map)
    pub2ref = pub2ref.dropna(subset=["CitingPublicationId", "CitedPublicationId"])

    log.info(
        f"  pub2ref  : {pub2ref.shape[0]:,} rows, "
        f"{pub2ref.memory_usage(deep=True).sum() / 1e6:.1f} MB"
    )
    log.info(
        f"  topics   : {topics.shape[0]:,} rows, "
        f"{topics.memory_usage(deep=True).sum() / 1e6:.1f} MB"
    )

    return pub2ref, topics


# =============================================================================
# BUILD pub2field FOR A GIVEN TOPIC LEVEL
# =============================================================================
def build_pub2field(topics, level):
    level_col = TOPIC_LEVEL_MAP[level]
    df = topics[["work_id", level_col]].dropna(subset=[level_col]).copy()
    df = df.rename(columns={"work_id": "PublicationId", level_col: "FieldId"})
    df = df.drop_duplicates(subset=["PublicationId", "FieldId"])
    df = downcast_numeric_df(df)
    df = strip_categories(df)

    n_fields = df["FieldId"].nunique()
    log.info(f"  Level {level} ({level_col}): {n_fields} unique fields")
    return df


# =============================================================================
# PRECOMPUTE DISTANCE MATRICES
# =============================================================================
def precompute_distance_matrices(pub2ref, topics, scenarios):
    """
    Pre-compute one distance matrix per unique (level, metric, temporal).
    All use citation_direction='references' since every scenario does.
    Caches to disk so repeated runs skip computation.
    """
    log.info("Pre-computing field distance matrices...")

    combos = {(s["level"], s["metric"], s["temporal"]) for s in scenarios}
    log.info(f"  {len(combos)} unique (level, metric, temporal) combinations")

    matrices = {}
    pub2field_cache = {}

    for level, metric, temporal in sorted(combos):
        key = (level, metric, temporal)
        cache_path = os.path.join(
            DIST_DIR,
            f"distance_L{level}_{metric}_{'temporal' if temporal else 'static'}.parquet",
        )

        if os.path.exists(cache_path):
            log.info(f"  Loading cached: {cache_path}")
            matrices[key] = pd.read_parquet(cache_path)
            continue

        if level not in pub2field_cache:
            pub2field_cache[level] = build_pub2field(topics, level)
        pub2field = pub2field_cache[level]

        t0 = time.time()
        log.info(
            f"  Computing: level={level}, metric={metric}, "
            f"temporal={temporal} ..."
        )

        distance_df = field_citation_distance(
            pub2ref=pub2ref,
            pub2field=pub2field,
            pub2field_norm=True,
            temporal=temporal,
            citation_direction="references",
            field_distance_metric=metric,
            show_progress=True,
        )

        elapsed = time.time() - t0
        log.info(f"    -> {distance_df.shape[0]:,} field pairs in {elapsed:.1f}s")

        pq.write_table(pa.Table.from_pandas(distance_df), cache_path)
        log.info(f"    -> Saved to {cache_path}")

        matrices[key] = distance_df

    del pub2field_cache
    gc.collect()
    return matrices


# =============================================================================
# CORE: CHUNK-OUTER, SCENARIO-INNER LOOP
#
# Structure:
#   for each chunk of citing publications:         (outer)
#       subset pub2ref ONCE
#       for each level (1, 2, 3):                  (middle)
#           subset pub2field ONCE per level
#           for each (metric, temporal) at level:   (inner)
#               look up pre-computed distance matrix
#               call raostriling_interdisciplinarity
#               write batch result
#
# This avoids:
#   - re-subsetting pub2ref 12 times (done once per chunk)
#   - re-subsetting pub2field 12 times (done 3 times per chunk)
# =============================================================================
def run_all_scenarios(pub2ref, topics, scenarios, batch_size, dist_matrices):
    log.info("=" * 60)
    log.info(f"Running {len(scenarios)} scenarios with chunk-outer loop")
    log.info("=" * 60)

    # ---- Group scenarios by level ----
    # {level: [scenario_dict, ...]}
    scenarios_by_level = defaultdict(list)
    for sc in scenarios:
        scenarios_by_level[sc["level"]].append(sc)

    active_levels = sorted(scenarios_by_level.keys())
    log.info(f"  Active levels: {active_levels}")

    # ---- Build pub2field tables (one per active level) ----
    pub2field_by_level = {}
    field_pubset_by_level = {}
    for level in active_levels:
        p2f = build_pub2field(topics, level)
        pub2field_by_level[level] = p2f
        field_pubset_by_level[level] = set(p2f["PublicationId"].values)

    # ---- Prepare result writers: one open list per scenario ----
    # We accumulate batch parquets and combine at the end.
    batch_files = defaultdict(list)  # scenario_id -> [filepath, ...]

    # ---- Sort pub2ref once for efficient subsetting ----
    pub2ref_sorted = pub2ref.sort_values("CitingPublicationId").reset_index(
        drop=True
    )
    all_citing_ids = pub2ref_sorted["CitingPublicationId"].unique()
    n_pubs = len(all_citing_ids)
    n_batches = ceil(n_pubs / batch_size)
    log.info(f"  {n_pubs:,} citing publications -> {n_batches} batches\n")

    total_start = time.time()

    for batch_idx in range(0, n_batches):
    #for batch_idx in range(0, 1):
        t_batch = time.time()
        chunk_ids = all_citing_ids[
            batch_idx * batch_size : (batch_idx + 1) * batch_size
        ]
        chunk_id_set = set(chunk_ids)

        # ============================================================
        # Subset pub2ref ONCE for this chunk (shared across all 12)
        # ============================================================
        chunk_pub2ref = pub2ref_sorted[
            pub2ref_sorted["CitingPublicationId"].isin(chunk_id_set)
        ]
        cited_in_chunk = set(chunk_pub2ref["CitedPublicationId"].unique())

        # ============================================================
        # Loop over levels (3), then scenarios within level (up to 4)
        # ============================================================
        for level in active_levels:
            # Subset pub2field ONCE per level per chunk
            relevant_ids = chunk_id_set | cited_in_chunk
            relevant_ids = relevant_ids & field_pubset_by_level[level]
            chunk_pub2field = pub2field_by_level[level][
                pub2field_by_level[level]["PublicationId"].isin(relevant_ids)
            ]

            for sc in scenarios_by_level[level]:
                # Get precomputed distance matrix if available
                distance_df = None if dist_matrices is None else dist_matrices.get(
                    (sc["level"], sc["metric"], sc["temporal"])
                )
            
                try:
                    # Compute Rao-Stirling interdisciplinarity
                    rs = raostriling_interdisciplinarity(
                        pub2ref=chunk_pub2ref,
                        pub2field=chunk_pub2field,
                        focus_pub_ids=None,
                        pub2field_norm=True,
                        temporal=sc["temporal"],
                        citation_direction="references",
                        field_distance_metric=sc["metric"],
                        distance_matrix=distance_df,
                        show_progress=True,
                    )
            
                    # Add scenario info
                    rs["Scenario"] = sc["Scenario"]
                    rs["topic_level"] = sc["level"]
            
                    # Save batch result
                    outpath = os.path.join(
                        OUTPUT_DIR,
                        f"scenario_{sc['Scenario']}_batch_{batch_idx}_{batch_size}.parquet",
                    )
                    pq.write_table(pa.Table.from_pandas(rs), outpath)
                    batch_files[sc["Scenario"]].append(outpath)
            
                except Exception as e:
                    # Log empty distance matrix or other issues
                    log.warning(f"Scenario {sc['Scenario']} skipped due to error: {e}")
            
                finally:
                    # Clean up memory
                    if 'rs' in locals():
                        del rs

            del chunk_pub2field

        del chunk_pub2ref
        gc.collect()

        # ---- Progress ----
        elapsed_total = time.time() - total_start
        elapsed_batch = time.time() - t_batch
        rate = elapsed_total / (batch_idx + 1)
        remaining = (n_batches - batch_idx - 1) * rate / 60

        log.info(
            f"  Chunk {batch_idx + 1}/{n_batches} | "
            f"{len(chunk_ids):,} pubs × {len(scenarios)} scenarios | "
            f"{elapsed_batch:.1f}s | "
            f"ETA ~ {remaining:.1f} min"
        )


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    # args --------------------------------------------
    import argparse

    parser = argparse.ArgumentParser(
        description="Compute Rao-Stirling interdisciplinarity across scenarios"
    )
    parser.add_argument("--run-all", action="store_true")
    parser.add_argument("--scenario", type=int, nargs='+', default=PARTIAL_SCENARIO)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()

    run_all = args.run_all if args and args.run_all is not None else RUN_ALL_SCENARIOS
    batch_size = args.batch_size if args and args.batch_size is not None else BATCH_SIZE

    if args.scenario:
        selected_scenarios = args.scenario
    else:
        selected_scenarios = PARTIAL_SCENARIO

    # Ensure it's always a list
    if isinstance(selected_scenarios, int):
        selected_scenarios = [selected_scenarios]

    # Select scenarios
    scenarios = SCENARIOS if run_all else [s for s in SCENARIOS if s["Scenario"] in selected_scenarios]
    # -----------------------------------------------------
    
    # run ---------------------------------------------------------
    # pub2ref, topics = load_inputs()

    pub2ref = pd.read_parquet("pub2ref_processed.parquet", engine='pyarrow').drop_duplicates()
    topics  = pd.read_parquet("topics_processed.parquet", engine='pyarrow').drop_duplicates()
    
    # Pre-compute distance matrices for selected scenarios only
    dist_matrices = precompute_distance_matrices(pub2ref, topics, scenarios)
    # Run with chunk-outer, scenario-inner structure
    run_all_scenarios(pub2ref, topics, scenarios, batch_size, dist_matrices)

    log.info("Done.")
    
    
# =================================
# Combine scenarios
# =================================
import os
import glob
import pandas as pd

SCENARIOS = [
    {"Scenario": 1,  "metric": "cosine",    "temporal": True,  "level": 1},
    {"Scenario": 2,  "metric": "euclidean", "temporal": True,  "level": 1},
    {"Scenario": 3,  "metric": "cosine",    "temporal": False, "level": 1},
    {"Scenario": 4,  "metric": "euclidean", "temporal": False, "level": 1},
    {"Scenario": 5,  "metric": "cosine",    "temporal": True,  "level": 2},
    {"Scenario": 6,  "metric": "euclidean", "temporal": True,  "level": 2},
    {"Scenario": 7,  "metric": "cosine",    "temporal": False, "level": 2},
    {"Scenario": 8,  "metric": "euclidean", "temporal": False, "level": 2},
    {"Scenario": 9,  "metric": "cosine",    "temporal": True,  "level": 3},
    {"Scenario": 10, "metric": "euclidean", "temporal": True,  "level": 3},
    {"Scenario": 11, "metric": "cosine",    "temporal": False, "level": 3},
    {"Scenario": 12, "metric": "euclidean", "temporal": False, "level": 3},
]

input_base = "raostirling_intermediate_results"
output_base = "multidisciplinary_index_final_results"
os.makedirs(output_base, exist_ok=True)

# Separate scenarios by temporal
temporal_true_scenarios = [s["Scenario"] for s in SCENARIOS if s["temporal"]]
temporal_false_scenarios = [s["Scenario"] for s in SCENARIOS if not s["temporal"]]

def combine_scenarios(scenario_list, temporal_flag):
    combined_list = []
    for s in scenario_list:
        files = glob.glob(f"{input_base}/scenario_{s}_*.parquet")
        if not files:
            print(f"No files for scenario {s}")
            continue
        dfs = [pd.read_parquet(f) for f in files]
        combined = pd.concat(dfs, ignore_index=True)
        
        # Save combined CSV
        out_path = f"{output_base}/scenario_{s}_combined_{'temporal' if temporal_flag else 'non_temporal'}.csv"
        combined.to_csv(out_path, index=False)
        print(f"Saved scenario {s} -> {out_path}")
        combined_list.append(combined)
    return combined_list

# Combine each group
dfs_temporal = combine_scenarios(temporal_true_scenarios, True)
dfs_non_temporal = combine_scenarios(temporal_false_scenarios, False)

# Now, normalize columns for combining
for i, df_nt in enumerate(dfs_non_temporal):
    # temporal=False has columns: RaoStirling, PublicationId, Scenario, topic_level
    # we want to match temporal=True: PublicationId, CitingYear, RaoStirling, Scenario, topic_level
    if "CitingYear" not in df_nt.columns:
        df_nt["CitingYear"] = pd.NA  # fill with NA to match temporal=True
    dfs_non_temporal[i] = df_nt[["PublicationId", "CitingYear", "RaoStirling", "Scenario", "topic_level"]]

# temporal=True should already have: PublicationId, CitingYear, RaoStirling, Scenario, topic_level
for i, df_t in enumerate(dfs_temporal):
    dfs_temporal[i] = df_t[["PublicationId", "CitingYear", "RaoStirling", "Scenario", "topic_level"]]

# Combine all together
all_combined = pd.concat(dfs_temporal + dfs_non_temporal, ignore_index=True)

# Keep CitingYear
citing_years = all_combined[["PublicationId", "CitingYear"]].dropna().drop_duplicates(subset=["PublicationId"])

# Pivot
pivot = all_combined.pivot_table(
    index=["PublicationId"], 
    columns="Scenario", 
    values="RaoStirling"
).reset_index()

pivot = pivot.merge(citing_years, on="PublicationId", how="left")

# Rename columns
pivot = pivot.rename(columns={c: f"Scenario_{int(c)}" for c in pivot.columns if isinstance(c, int)})

# Save final pivot
pivot.to_csv(f"{output_base}/all_scenarios.csv", index=False)
print(pivot.head())

