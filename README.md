# OpenAlex-diversity-multidisciplinarity

Large-scale data pipelines, including data linkage with OpenAlex publication and citation metadata to analyse research team structure, collaboration, and interdisciplinarity.

It combines advanced entity resolution, bibliometric data engineering, and topic extraction to construct a unified dataset of authors, institutions, publications, and referenced works.

---

### Entity Resolution & Matching Pipeline

A scalable matching system resolves identities across datasets:

- Exact matching 
- Token-based blocking heuristics  
- Fuzzy string matching (Jaro-Winkler distance)  

---

### OpenAlex Data Extraction

A parallelised data retrieval system linking publication metadata using DOIs or OpenAlex IDs, with:

- Multithreaded API requests  
- Batch processing for large datasets  
- Incremental Parquet checkpointing for fault tolerance  

---

### System Design

The pipeline is optimised for large datasets using PostgreSQL, temporary tables, and parallel processing, producing clean and usable outputs for research evaluation and bibliometric analysis.  
