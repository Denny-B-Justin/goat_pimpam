# GoAT — Governance & Analytics Tool
### Final Data Preparation Pipeline

> **Team:** Public Investment Management & Public Asset Management (PIM-PAM), World Bank  
> **Runtime:** Databricks (PySpark + Unity Catalog) · Python 3.x  
> **Notebook:** `GoAT_Final_Data_Preparation.ipynb`

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Sources](#data-sources)
4. [Keyword Taxonomy Hierarchies](#keyword-taxonomy-hierarchies)
5. [Pipeline Stages](#pipeline-stages)
6. [Configuration Parameters](#configuration-parameters)
7. [Core Functions](#core-functions)
8. [Output Datasets](#output-datasets)
9. [Visualizations](#visualizations)
10. [Dependencies](#dependencies)
11. [Environment Variables](#environment-variables)

---

## Overview

GoAT is a keyword-driven project intelligence pipeline that mines the World Bank project portfolio for evidence of PFM, PIMA, Decentralization, Asset Management, DRM, and Procurement activities. It operates across four distinct document sources — Project Development Objectives (PDOs), Prior Actions, full-text PAD documents, and Results Indicators — tagging each project against a configurable hierarchical keyword taxonomy. The output is a structured set of project-level analytical datasets consumed by downstream dashboards and portfolio reviews.

---

## Architecture

```
Runtime Config (sel_hierarchy · country · year · sel_projects)
        │
        ▼
Keyword Taxonomy Builder  ──────────────────────────────────────────────────┐
        │                                                                    │
        ▼                                                                    │
PROJECT_MASTER_V3  (PySpark CSV load → remove_empty_top_cells → toPandas)  │
        │                                                                    │
        ▼                                                                    │
Project Portfolio Filter                                                     │
  ├─ Lending instrument: DPL, IPF, PforR                                    │
  ├─ Status: Active | Closed                                                 │
  ├─ Country: all | list                                                     │
  └─ Approval FY: min_year – max_year                                       │
        │                                                                    │
        ├──────────┬───────────────┬──────────────────────────────┐         │
        ▼          ▼               ▼                              ▼         │
  PDO text   Prior Actions   PAD full-text (VMLPADImageBank)  Results Inds  │
        │          │               │                              │         │
        └──────────┴───────────────┴──────────────────────────────┘         │
                                   │         ◄──────────────────────────────┘
                                   ▼  check_pfm_categories / check_keyword
                         Category Tagging & Deduplication
                           (dropna all · groupby PROJ_ID · Yes/No collapse)
                                   │
                                   ▼
                           Sector Join (SECTOR_04_09_2026.csv)
                                   │
                                   ▼
                         Analytical Output Datasets
                         (df_result_cleaned · df_pdo_search_selected
                          · df_results_indicators_extracted)
                                   │
                                   ▼
                         Plotly Express Visualizations
```

---

## Data Sources

| Source | Format | Path / URL | Key Fields |
|--------|--------|-----------|-----------|
| `PROJECT_MASTER_V3` | CSV (Unity Catalog Volume) | `/Volumes/prd_mega/saiana95/vaiana95/GOAT/PROJECT_MASTER_V3.csv` | `PROJ_ID`, `PROJ_DEV_OBJECTIVE_DESC`, `PROJ_APPRVL_FY`, `LNDNG_INSTR_LONG_NAME`, `PROJ_STAT_NAME`, `CNTRY_SHORT_NAME`, `LEAD_GP_NAME`, `CMT_AMT`, `DLI_IND` |
| `Prior Actions DB` | XLSX (World Bank public) | `https://thedocs.worldbank.org/…/DPADdatabaseFY22.xlsx` | `Project ID`, `TEXT` |
| `PAD Image Bank` | Spark table (Delta) | `VIETNAMML.VMLPADImageBankDocuments` | `project_id`, `fullText`, `document_type`, `lending_instrument`, `language`, `datee` |
| `Results Indicators` | CSV (Unity Catalog Volume) | `/Volumes/prd_mega/saiana95/vaiana95/GOAT/PROJECT_RESULT_IND_DETAIL_V2_04_09_2026.csv` | `PROJ_ID`, `IND_TYPE_NAME`, `IND_NAME`, `BASELINE_VAL_TEXT`, `TGT_VAL_TEXT`, `UOM_NAME` |
| `Sector Classification` | CSV (Unity Catalog Volume) | `/Volumes/prd_mega/saiana95/vaiana95/GOAT/SECTOR_04_09_2026.csv` | `PROJ_ID`, `SECT_TEXT` |

> All CSVs in Unity Catalog use a non-standard 4-row header offset; the `remove_empty_top_cells(df, 4)` helper handles this uniformly.

---

## Keyword Taxonomy Hierarchies

GoAT's taxonomy is a nested `dict[str, list[str]]` mapping category names to keyword lists. The active hierarchy is selected via `sel_hierarchy`.

### `pima` — Public Investment Management Assessment
```python
{
  'Key Terms':  ['public investment management'],
  'Additional': ['public investment management assessment',
                 'climate public investment management']
}
```

### `pfm` — Public Financial Management
| Category | Sample Keywords |
|----------|----------------|
| PFM Reform and Diagnostic Tools | `PEFA`, `CPIA`, `PFM Reform`, `PFM Coordination` |
| Transparency, Participation and Accountability | `budget transparency`, `BOOST`, `open contracting data standards` |
| Procurement | `e-procurement`, `OCDS`, `green procurement` |
| Policy-based fiscal strategy and budgeting | `MTEF`, `Program-budgeting`, `gender budgeting` |
| Public Investment and Asset Management | `PIM`, `PIMA`, `Asset Management`, `PIP` |
| Predictability and control in budget execution | `IFMIS`, `TSA`, `Internal audit`, `cash management` |
| Accounting and Reporting | `IPSAS`, `GFS`, `COFOG`, `Accrual` |
| External Scrutiny and Audit | `External audit`, `supreme audit institution` |
| Financing of Service Delivery | `conditional grant`, `equalization grant`, `allocation formula` |
| Climate PFM & PIM | `climate tagging`, `green PIM`, `climate budget` |

### `decentralization`
| Category | Sample Keywords |
|----------|----------------|
| Intergovernmental Reform | `intergovernmental relations`, `local self government` |
| Intergovernmental fiscal transfers | `conditional grant`, `allocation formulae` |
| Locally raised revenue | `own source revenue`, `property tax`, `cadaster` |
| Subnational borrowing | `municipal bonds`, `local government debt` |
| Local Service Delivery | `results based financing`, `district health services` |

### `asset management`
```python
{
  'Key Terms':  ['asset management', 'infrastructure governance', 'asset register'],
  'Additional': ['asset governance', 'public asset management', 'asset valuation']
}
```

### `drm` — Domestic Revenue Mobilization
```python
{ 'Key Terms': ['domestic revenue mobilization', 'domestic revenue mobilisation'] }
```

### `procurement`
```python
{ 'Key Terms': ['procurement'] }
```

---

## Pipeline Stages

### Stage 1 — Runtime Configuration

```python
sel_projects   = 'all'      # 'all' | 'selected' (uses list_projects)
sel_hierarchy  = 'pima'     # 'pima' | 'pfm' | 'decentralization' | 'asset management' | 'drm' | 'procurement'
country_selected = 'all'    # 'all' | ['Country1', 'Country2']
min_year = 2000
max_year = 2026
```

### Stage 2 — Keyword Taxonomy Builder

`hierarchy = hierarchy_tagging[sel_hierarchy]` selects the active dictionary.  
A flat `search_term_list` is derived by unpacking all category word-lists for use in PAD sentence extraction.

### Stage 3 — Project Master Ingestion

```python
df_project_master = spark.read.format('csv') \
    .option("delimiter", ",").option("header", "false") \
    .load('/Volumes/prd_mega/saiana95/vaiana95/GOAT/PROJECT_MASTER_V3.csv')
df_project_master = remove_empty_top_cells(df_project_master, 4)
df_project_master_subset = df_project_master.toPandas()
```

Column subset retained:
`PROJ_ID`, `PROJ_DISPLAY_NAME`, `PROJ_APPRVL_FY`, `PROJ_DEV_OBJECTIVE_DESC`, `RGN_NAME`, `PROJ_STAT_NAME`, `CNTRY_SHORT_NAME`, `PROD_LINE_NAME`, `LNDNG_INSTR_TYPE_CODE`, `LNDNG_INSTR_LONG_NAME`, `CMT_AMT`, `PARENT_PROJ_ID`, `TEAM_LEAD_FULL_NAME`, `LEAD_GP_NAME`, `PROJ_SHORT_NAME`, `PROJ_LGL_NAME`, `PROD_LINE_TYPE_NAME`, `DLI_IND`

### Stage 4 — Project Portfolio Filter

Applied sequentially to `df_project_master_subset`:

```python
# Exclude manually flagged project
df = df[df['PROJ_ID'] != 'P169212']

# Lending instrument gate
df = df[df['LNDNG_INSTR_LONG_NAME'].isin([
    'Development Policy Lending',
    'Investment Project Financing',
    'Program-for-Results Financing'
])]

# Status gate
df = df[df['PROJ_STAT_NAME'].isin(['Closed', 'Active'])]

# Optional country filter
if country_selected != 'all':
    df = df[df['CNTRY_SHORT_NAME'].isin(country_selected)]

# Fiscal year window
df = df[df['PROJ_APPRVL_FY'].astype(int).between(min_year, max_year)]
```

### Stage 5 — Multi-Source Keyword Search

Each source arm runs `check_pfm_categories` (or `check_keyword` for PAD full-text), returning `'Yes'` / `None` per hierarchy category.

**PDO search:**
```python
for each_group in hierarchy:
    df_pdo_search[each_group] = df_pdo_search['PROJ_DEV_OBJECTIVE_DESC'] \
        .apply(check_pfm_categories, category=each_group)
df_pdo_search_selected = df_pdo_search.set_index([...]).dropna(how='all').reset_index()
```

**PAD full-text (sentence-level extraction):**  
`check_keyword` accesses segment `fullText[24]`, splits on `.`, and returns concatenated context sentences where any search term matches. This provides sentence-level evidence alongside the boolean flag.

**Results Indicators:**  
Matches against `IND_NAME` for both PDO-level and intermediate results indicators.

### Stage 6 — Category Tagging & Deduplication

```python
df_result_cleaned = df_result_sel.groupby(['PROJ_ID', 'document_type']).agg(list)

def clean_data(x):
    return 'Yes' if 'Yes' in x else 'No'

for col in df_result_cleaned.columns:
    df_result_cleaned[col] = df_result_cleaned[col].apply(clean_data)

df_result_cleaned = df_result_cleaned.reset_index().drop_duplicates()
```

### Stage 7 — Sector Join

```python
df_proj_sector = spark.read.format('csv') \
    .option("delimiter", ",").option("header", "false") \
    .load('/Volumes/prd_mega/saiana95/vaiana95/GOAT/SECTOR_04_09_2026.csv')
df_proj_sector = remove_empty_top_cells(df_proj_sector, 4).toPandas()
df_proj_sector = df_proj_sector[df_proj_sector['PROJ_ID'].isin(proj_ids)] \
    .groupby('PROJ_ID')['SECT_TEXT'].apply(list).reset_index().dropna()

df_result_set = pd.merge(df_project_master_subset, df_proj_sector, on='PROJ_ID')
```

---

## Configuration Parameters

| Parameter | Type | Values | Description |
|-----------|------|--------|-------------|
| `sel_projects` | `str` | `'all'`, `'selected'` | Scope of project search |
| `sel_hierarchy` | `str` | `'pima'`, `'pfm'`, `'decentralization'`, `'asset management'`, `'drm'`, `'procurement'` | Active keyword taxonomy |
| `country_selected` | `str` \| `list` | `'all'`, `['Zambia', 'Kenya']` | Country filter |
| `min_year` | `int` | e.g. `2000` | Approval FY lower bound (inclusive) |
| `max_year` | `int` | e.g. `2026` | Approval FY upper bound (inclusive) |

---

## Core Functions

### `remove_empty_top_cells(df, header_row_index)`
Strips non-data header rows from World Bank CSV exports that use a 4-row offset before column names begin.

```python
def remove_empty_top_cells(df: DataFrame, header_row_index: int) -> DataFrame:
    df_with_id = df.withColumn("row_id", F.monotonically_increasing_id())
    header_row = df_with_id.orderBy("row_id").limit(header_row_index + 1).collect()[header_row_index]
    new_columns = header_row[:-1]
    df_clean = df_with_id.orderBy("row_id") \
        .filter(F.col("row_id") > header_row_index) \
        .drop("row_id") \
        .toDF(*new_columns)
    return df_clean
```

### `check_pfm_categories(x, category)`
Boolean keyword match — returns `'Yes'` if any word from `hierarchy[category]` is present in text `x`, else `None`.

```python
def check_pfm_categories(x, category):
    if x is not None:
        return 'Yes' if any(kw in x for kw in hierarchy[category]) else None
    return None
```

### `check_keyword(x, words)`
Sentence-level context extractor for PAD full-text. Accesses the 25th text segment (`fullText[24]`), iterates over sentences, and returns concatenated context (preceding sentence + matching sentence) for all keyword hits.

```python
def check_keyword(x, words):
    if isinstance(x, float):
        return 'PAD Not Available'
    if len(x) >= 24:
        sentences = x[24].split('.')
        result = ''
        for n, s in enumerate(sentences):
            if any(kw in s for kw in words):
                result += sentences[n-1] + '. ' + s
        return str(result).replace(',', '/').replace(';', '')
    return None
```

### `clean_data(x)`
Collapses a list of `'Yes'`/`'No'` values (post-groupby agg) to a single canonical flag.

```python
def clean_data(x):
    return 'Yes' if 'Yes' in x else 'No'
```

---

## Output Datasets

| Dataset | Description | Key Columns |
|---------|-------------|------------|
| `df_result_cleaned` | PAD-sourced tagging: one row per `(PROJ_ID, document_type)` with Yes/No per hierarchy category | `PROJ_ID`, `document_type`, `[category_cols]` |
| `df_pdo_search_selected` | Projects matched via PDO keyword search | `PROJ_ID`, `PROJ_DEV_OBJECTIVE_DESC`, `[category_cols]` |
| `df_prior_actions_selected` | DPL projects matched via Prior Action text | `Project ID`, `TEXT`, `[category_cols]` |
| `df_results_indicators_extracted` | Projects matched via Results Indicator names | `PROJ_ID`, `IND_TYPE_NAME`, `IND_NAME`, `BASELINE_VAL_TEXT`, `TGT_VAL_TEXT`, `UOM_NAME`, `[category_cols]` |

All category columns use binary `'Yes'` / `'No'` encoding.

---

## Visualizations

The notebook produces the following Plotly Express charts:

| Chart | X-axis | Y-axis | Color dimension |
|-------|--------|--------|----------------|
| Portfolio trend — full universe | `PROJ_APPRVL_FY` | project count | `LNDNG_INSTR_LONG_NAME` |
| Portfolio trend — by status | `PROJ_APPRVL_FY` | project count | `PROJ_STAT_NAME` |
| Portfolio by product line | `PROD_LINE_NAME` | project count | `LNDNG_INSTR_LONG_NAME` |
| PDO-matched — by status | `PROJ_APPRVL_FY` | project count | `PROJ_STAT_NAME` |
| PDO-matched — by country & GP | `CNTRY_SHORT_NAME` | project count | `LEAD_GP_NAME` |
| PA-matched — by status | `PROJ_APPRVL_FY` | project count | `PROJ_STAT_NAME` |
| PA-matched — by country & GP | `CNTRY_SHORT_NAME` | project count | `LEAD_GP_NAME` |

---

## Dependencies

```python
# Databricks runtime (Spark 3.x)
pyspark
delta-spark

# Python packages
pandas>=1.5
openpyxl           # Excel read (Prior Actions DB)
plotly>=5.0        # Plotly Express visualizations
python-dotenv      # .env loading for PROJECT_MASTER_CSV_URL
```

Install in Databricks notebook:

```python
%pip install python-dotenv openpyxl
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PROJECT_MASTER_CSV_URL` | URL or path override for `PROJECT_MASTER_V3.csv` (optional; loaded via `python-dotenv`) |

Set in `.env` at the repo root or as a Databricks cluster environment variable:

```bash
PROJECT_MASTER_CSV_URL=https://...
```

---

## Notes

- The PAD full-text extraction accesses `fullText[24]` (the 25th document segment), which empirically corresponds to the main body text in the World Bank ImageBank document schema. This offset may need updating if the upstream document structure changes.
- The `P169212` exclusion is a hard-coded project-level override. Document the reason for exclusion in team records.
- For very large portfolios, consider moving `check_pfm_categories` to a vectorised Pandas `str.contains` pattern or a PySpark UDF to reduce per-row Python overhead.
- PAD language filtering (`language == 'English'`) and project-ID explosion (`explode('project_id')`) are applied before the keyword search to prevent false matches in non-English documents and multi-project PADs.