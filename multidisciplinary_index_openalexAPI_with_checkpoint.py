import os
from pathlib import Path 
import polars as pl
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set working path
path = Path().resolve()
os.chdir(path)

# Load CSV and DOIs
header_df = pd.read_csv("./gtr_publications_2025_11.csv", nrows=0, encoding="utf-8")
df = pd.read_csv(
    "./gtr_publications_2025_11.csv", header=None, skiprows=1,
    encoding="utf-8", usecols=range(header_df.shape[1])
)
df.columns = header_df.columns
df["DOI"] = df["DOI"].str.strip()
df = df[df["DOI"].notna() & (df["DOI"] != "")]

year = 2019
df_year = df[df['Year'] == year]
dois = df_year["DOI"].tolist()
doi_to_year = dict(zip(df_year["DOI"], df_year["Year"]))

# =====================
# Fetch functions
# =====================
def _fetch_batch(batch, id_type, debug=False):
    # Prepare batch of IDs based on type (DOI or OpenAlex ID)
    if id_type == "doi":
        # Encode DOIs as URLs for the API
        batch_enc = [f"https://doi.org/{d}" for d in batch]
        filter_param = "doi"
    elif id_type == "openalex":
        # OpenAlex IDs can be used directly
        batch_enc = batch
        filter_param = "ids.openalex"
    else:
        # Raise error if id_type is invalid
        raise ValueError("id_type not 'doi' or 'openalex id'")

    # Construct the OpenAlex API URL for the batch
    url = f"https://api.openalex.org/works?filter={filter_param}:{'|'.join(batch_enc)}&per-page={len(batch)}"

    # Optionally print debug information
    if debug:
        print(f"\n=== OPENALEX BATCH DEBUG ===\nID Type: {id_type}\nURL: {url}\n===========================\n")

    # Send GET request to OpenAlex API
    resp = requests.get(url, headers={"Accept": "application/json"}, timeout=30)
    resp.raise_for_status()  # Raise exception if request failed
    
    # Return the list of works from the API response
    return resp.json().get("results", [])


def fetch_openalex_works_parallel(ids, id_type="doi", batch_size=50, max_workers=5, debug=False,
                                 checkpoint_path=None, checkpoint_every=100):
    all_works = []

    # Load checkpoint if exists to avoid re-fetching already downloaded works
    if checkpoint_path and os.path.exists(checkpoint_path):
        df_existing = pd.read_parquet(checkpoint_path)
        fetched_ids = set(df_existing['WorkOpenAlexID'].tolist())
        # Filter out already fetched IDs
        ids = [i for i in ids if i not in fetched_ids]
        all_works.extend(df_existing.to_dict('records'))
        print(f"Loaded {len(fetched_ids):,} works from checkpoint, fetching {len(ids):,} more.")

    # Split IDs into batches for parallel fetching
    batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]

    # Use ThreadPoolExecutor for parallel requests
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_fetch_batch, batch, id_type, debug) for batch in batches]

        # Process completed futures as they finish
        for i, future in enumerate(as_completed(futures), start=1):
            try:
                works = future.result()
                # Rename 'id' field to 'WorkOpenAlexID' for consistency
                for w in works:
                    w['WorkOpenAlexID'] = w.pop('id')
                all_works.extend(works)
            except Exception as e:
                # Handle any batch failures without stopping the whole process
                print("Batch failed:", e)

            # Save checkpoint periodically or at the end
            if checkpoint_path and (i % checkpoint_every == 0 or i == len(batches)):
                pd.DataFrame(all_works).to_parquet(checkpoint_path, engine="pyarrow", compression="zstd", index=False)
                print(f"Checkpoint saved after {i}/{len(batches)} batches ({len(all_works)} works) → {checkpoint_path}")

    # Return the full list of fetched works
    return all_works

# =====================
# Topic extraction
# =====================
def extract_top_topics(topics, min_score=0.8, top_n=5):
    # Filter topics to only include those with a score higher than min_score
    high_score_topics = [t for t in topics if t.get("score", 0) >= min_score]
    
    # Sort the high-score topics in descending order and select the top N
    top_topics = sorted(high_score_topics, key=lambda x: x.get("score", 0), reverse=True)[:top_n]
    
    # Return a list of tuples with topic ID, name, score, and subfield info
    return [(t.get("id"), t.get("display_name"), t.get("score"),
             t.get("subfield", {}).get("id"), t.get("subfield", {}).get("display_name"))
            for t in top_topics]


# =========================
# Build publication to reference table
# =========================
def build_pub2ref_table_flat(works):
    # Initialize lists to store rows for the reference table and topic table
    rows, pub_topics = [], []

    # Iterate over each work (publication)
    for work in works:
        work_id = work.get("id")
        refs = work.get("referenced_works", [])
        ref_count = work.get("referenced_works_count", 0)
        pub_year = work.get("Year")

        # If there are no references, add a row with None as ReferencedWorkID
        if not refs:
            rows.append({"WorkOpenAlexID": work_id, "ReferencedWorkID": None, "ReferencedWorksCount": ref_count})
        else:
            # If there are references, add a row for each referenced work
            rows.extend([{"WorkOpenAlexID": work_id, "ReferencedWorkID": r, "ReferencedWorksCount": ref_count} for r in refs])

        # Extract top topics for the work and add them to the pub_topics table
        for t in extract_top_topics(work.get("topics", [])):
            pub_topics.append({
                "WorkOpenAlexID": work_id,
                "TopicID": t[0],
                "TopicName": t[1],
                "Score": t[2],
                "SubfieldID": t[3],
                "SubfieldName": t[4],
                "Year": pub_year,
                "Type": "publication"
            })

    # Convert the lists of dictionaries into pandas DataFrames and return
    return pd.DataFrame(rows), pd.DataFrame(pub_topics)


# =========================
# Build referenced works topic table
# =========================
def build_ref2field_table(
        pub2ref_df, 
        batch_size=50, 
        max_workers=5, 
        debug=False,
        checkpoint_path=None,           # main final checkpoint (per work)
        checkpoint_every=500,           # save every N works to main checkpoint
        checkpoint_batch_every=100,     # save every N batches inside fetch_with_audit
        inner_checkpoint_path=None      # incremental checkpoint for fetch_with_audit
    ):
    """
    Build a table of top 5 topics for all referenced works using pub2ref_df.
    Implements a two-pass fetch with checkpointing.

    Returns:
    - DataFrame with columns:
        ['WorkOpenAlexID', 'TopicID', 'TopicName', 'Score', 
         'SubfieldID', 'SubfieldName', 'Type', 'Year']
    """
    all_refs = pub2ref_df["ReferencedWorkID"].dropna().unique().tolist()
    print(f"Fetching {len(all_refs):,} referenced works...")

    rows = []
    already_fetched = set()
    if checkpoint_path and os.path.exists(checkpoint_path):
        df_existing = pd.read_parquet(checkpoint_path)
        rows.extend(df_existing.to_dict("records"))
        already_fetched = set(df_existing["WorkOpenAlexID"].tolist())
        all_refs = [r for r in all_refs if r not in already_fetched]
        print(f"Loaded {len(already_fetched):,} already fetched referenced works")

    def fetch_with_audit(ids, inner_checkpoint_path, checkpoint_batch_every):
        works = fetch_openalex_works_parallel(
            ids=ids,
            id_type="openalex",
            batch_size=batch_size,
            max_workers=max_workers,
            debug=debug,
            checkpoint_path=inner_checkpoint_path,
            checkpoint_every=checkpoint_batch_every
        )
        fetched_ids = {w["WorkOpenAlexID"] for w in works}
        missing_ids = set(ids) - fetched_ids
        return works, missing_ids

    # --- PASS 1 ---
    works_1, missing_1 = fetch_with_audit(all_refs, inner_checkpoint_path=inner_checkpoint_path,
                                          checkpoint_batch_every=checkpoint_batch_every)
    print(f"PASS 1 fetched: {len(works_1):,}, missing: {len(missing_1):,}")

    # --- PASS 2 (repair fetch) ---
    works_2, missing_2 = [], set()
    if missing_1:
        print("Starting PASS 2 (repair fetch)...")
        works_2, missing_2 = fetch_with_audit(list(missing_1),
                                              inner_checkpoint_path=inner_checkpoint_path,
                                              checkpoint_batch_every=checkpoint_batch_every)
        print(f"PASS 2 recovered: {len(works_2):,}, still missing: {len(missing_2):,}")

    all_works = works_1 + works_2

    if debug and missing_2:
        print(f"WARNING: {len(missing_2)} referenced works could not be fetched.")
        print("Example missing IDs:", list(missing_2)[:5])

    # --- Extract topics and save main checkpoint per work ---
    for i, work in enumerate(all_works, start=1):
        work_id = work.get("WorkOpenAlexID")
        pub_year = work.get("publication_year")
        for t in extract_top_topics(work.get("topics", [])):
            rows.append({
                "WorkOpenAlexID": work_id,
                "TopicID": t[0],
                "TopicName": t[1],
                "Score": t[2],
                "SubfieldID": t[3],
                "SubfieldName": t[4],
                "Type": "referenced",
                "Year": pub_year
            })

        if checkpoint_path and i % checkpoint_every == 0:
            pd.DataFrame(rows).to_parquet(checkpoint_path, engine="pyarrow", compression="zstd")
            if debug:
                print(f"Checkpoint saved at {i} works → {checkpoint_path}")

    # --- Final save ---
    df = pd.DataFrame(rows)
    if checkpoint_path:
        df.to_parquet(checkpoint_path, engine="pyarrow", compression="zstd")
    print(f"Total referenced topics extracted: {len(df):,}")
    return df


# =========================
# Build full publication + referenced works topic table
# =========================
def build_full_topic_table(
        pub_works, 
        batch_size=50, 
        max_workers=5, 
        debug=False,
        pub_checkpoint_path=None,
        full_checkpoint_path=None,   # save combined table
        ref_checkpoint_path=None,    # save only referenced topics
        inner_checkpoint_path=None,  # incremental checkpoint for referenced fetch
        checkpoint_every=500,        # save every N works to main checkpoint
        checkpoint_batch_every=100   # save every N batches in fetch_with_audit
    ):
    """
    Builds the full combined topic table for publications and referenced works with checkpointing.
    
    Returns:
    - full_topics_df: combined publications + referenced works
    - pub2ref_df: mapping of publications to referenced works
    """
    # Step 1: Build pub2ref table + publication topics
    pub2ref_df, pub_topics_df = build_pub2ref_table_flat(pub_works)

    if pub_checkpoint_path:
        pub_topics_df.to_parquet(pub_checkpoint_path, engine="pyarrow", compression="zstd", index=False)
        print(f"Publication topics saved: {len(pub_topics_df):,} rows → {pub_checkpoint_path}")

    # Step 2: referenced works
    ref_topics_df = build_ref2field_table(
        pub2ref_df,
        batch_size=batch_size,
        max_workers=max_workers,
        debug=debug,
        checkpoint_path=ref_checkpoint_path,           # main final checkpoint
        checkpoint_every=checkpoint_every,            # per work
        checkpoint_batch_every=checkpoint_batch_every,  # per batch inside fetch
        inner_checkpoint_path=inner_checkpoint_path   # incremental checkpoint
    )

    # Step 3: combine
    full_topics_df = pl.concat([pl.from_pandas(pub_topics_df), pl.from_pandas(ref_topics_df)])

    # final save
    if full_checkpoint_path:
        full_topics_df.write_parquet(full_checkpoint_path, compression="zstd")
        print(f"Full topics table saved {full_topics_df.height:,} rows → {full_checkpoint_path}")

    return full_topics_df, pub2ref_df

# ===========================================================


# >>>>>>>>>>>>>>>>>>>>>>>>>
# calling functions
# >>>>>>>>>>>>>>>>>>>>>>>>>
# --- Step 1: Fetch publication works in parallel ---
pub_works = fetch_openalex_works_parallel(
    ids=dois,
    id_type="doi",
    batch_size=50,      
    max_workers=5,      
    debug=True,
    checkpoint_path="checkpoints/pub_works_checkpoint.parquet",  # incremental save
    checkpoint_every=50  # save every 50 batches
)

# Attach Year from CSV to each publication once
for work in pub_works:
    doi = work.get("doi")
    work['Year'] = doi_to_year.get(doi, None)

print(f"Fetched {len(pub_works):,} publication works")

# --- Step 2: Build full topic and reference table ---
full_topics_df, pub2ref_df = build_full_topic_table(
    pub_works,
    batch_size=50,
    max_workers=5,
    debug=True,
    pub_checkpoint_path="checkpoints/publication_topics.parquet",   # publication topics checkpoint
    ref_checkpoint_path="checkpoints/referenced_topics.parquet",    # referenced topics checkpoint (final)
    inner_checkpoint_path="checkpoints/referenced_topics_raw_retrieval.parquet",  # incremental per-batch checkpoint
    full_checkpoint_path="checkpoints/full_topics.parquet",         # final combined table
    checkpoint_every=500,        # save every N works to main checkpoint
    checkpoint_batch_every=100   # save every N batches inside fetch_with_audit
)

print("Full topic table preview:")
print(full_topics_df.head(10))

print("Publication to reference table preview:")
print(pub2ref_df.head(10))
