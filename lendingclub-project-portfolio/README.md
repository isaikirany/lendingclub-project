# LendingClub Data Pipeline 🏦
### Multi-Dataset PySpark Cleaning Pipeline with Environment-Aware Execution

> A production-structured PySpark pipeline that ingests, validates, and cleans four LendingClub datasets — customers, loans, repayments, and defaulters. Modular transformation chain, explicit schema definitions, environment-aware config system, and structured Log4j logging throughout.

---

## Overview

This pipeline processes raw LendingClub loan data through four independent cleaning chains, each targeting a distinct dataset. Every transformation is implemented as a pure, testable function in `DataManipulation.py`. The entry point (`application_main.py`) orchestrates the chain using PySpark's native `.transform()` API, keeping the pipeline readable and the transformations independently reusable.

Environment switching (LOCAL → TEST → PROD) is a single command-line argument. No code changes needed.

---

## Architecture

```
data/raw/ (CSV source files)
        │
        ▼
┌───────────────────────────────────────────────────────┐
│                  LendingClub Pipeline                 │
│                                                       │
│   ConfigReader ──► application.conf + pyspark.conf   │
│                                  │                    │
│                    Utils.get_spark_session(env)       │
│                                  │                    │
│      ┌───────────────────────────┼──────────────┐     │
│      │           DataReader      │              │     │
│      │  (explicit StructType schemas)           │     │
│      └───────────────────────────┬──────────────┘     │
│                                  │                    │
│      ┌───────────────────────────▼──────────────┐     │
│      │         DataManipulation                 │     │
│      │                                          │     │
│      │  Customers:                              │     │
│      │   rename → ingest → distinct             │     │
│      │   → null filter → emp_length clean       │     │
│      │   → avg imputation → state validate      │     │
│      │                                          │     │
│      │  Loans:                                  │     │
│      │   ingest → filter → term parse           │     │
│      │   → purpose standardise                  │     │
│      │                                          │     │
│      │  Repayments:                             │     │
│      │   ingest → filter → null fill            │     │
│      │   → total derive → date parse ×2         │     │
│      │                                          │     │
│      │  Defaulters:                             │     │
│      │   ingest+fill → delinquency split        │     │
│      │              → public records split      │     │
│      └───────────────────────────┬──────────────┘     │
└─────────────────────────────────┼─────────────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │   data/cleaned/ (Parquet)   │
                    │   customers/                │
                    │   loans/                    │
                    │   loan_repayments/          │
                    │   loan_defaulters_delinquency/     │
                    │   loan_defaulters_public_records/  │
                    └─────────────────────────────┘
```

---

## Datasets

Source: [LendingClub public dataset on Kaggle](https://www.kaggle.com/datasets/wordsforthewise/lending-club)

| Dataset | Raw File | Key Fields |
|---|---|---|
| Customers | `customers.csv` | member_id, emp_length, annual_income, address_state, grade |
| Loans | `loans.csv` | loan_id, member_id, loan_amount, loan_status, loan_purpose, loan_term_months |
| Repayments | `loan_repayments.csv` | loan_id, total_payment_received, last/next payment date |
| Defaulters | `loan_defaulters.csv` | member_id, delinq_2yrs, pub_rec, inq_last_6mths, mths_since_last_delinq |

Place raw CSVs in `data/raw/` before running. They are excluded from version control via `.gitignore`.

---

## Transformation Details

### Customers
| Step | Function | What it does |
|---|---|---|
| 1 | `rename_customer_columns` | Expands abbreviated column names to readable form |
| 2 | `customers_df_ingested` | Adds `ingest_date` timestamp for lineage tracking |
| 3 | `customers_df_distinct` | Removes fully duplicate rows |
| 4 | `customers_income_filtered` | Drops records with null `annual_income` (required for loan analysis) |
| 5 | `customers_emplength_cleaned` | Strips non-numeric chars from `emp_length` ('10+ years' → 10), casts to int |
| 6 | `avg_emp_length_imputed` | Fills null `emp_length` with average within loan grade group; global mean as fallback |
| 7 | `customers_state_cleaned` | Uppercases and trims `address_state`; filters out invalid US state codes |

### Loans
| Step | Function | What it does |
|---|---|---|
| 1 | `loans_ingested` | Adds `ingest_date` timestamp |
| 2 | `loans_filtered` | Drops records with null loan_id/member_id, zero loan_amount, or unknown loan_status |
| 3 | `loans_term_modified` | Strips '36 months' → 36, casts to int, nullifies non-positive values |
| 4 | `loans_purpose_modified` | Lowercases, underscores spaces, maps rare categories to 'other' |

### Repayments
| Step | Function | What it does |
|---|---|---|
| 1 | `repayments_ingested` | Adds `ingest_date` timestamp |
| 2 | `repayments_filtered` | Drops records with null loan_id or zero/null total_payment_received |
| 3 | `repayments_null_amounts_filled` | Fills null payment sub-components with 0.0 (null = no charge, not missing) |
| 4 | `repayments_total_derived` | Derives total_payment_received = principal + interest + late_fee where null |
| 5 | `repayments_last_date_parsed` | Parses `last_payment_date` from 'MMM-yyyy' string to DateType |
| 6 | `repayments_next_date_parsed` | Parses `next_payment_date` from 'MMM-yyyy' string to DateType |

### Defaulters
| Step | Function | What it does |
|---|---|---|
| 1 | `defaulters_ingested` | Fills all null numeric risk fields with 0.0, adds `ingest_date` |
| 2 | `defaulters_delinquency` | Flags members with active delinquency signals (delinq_2yrs, delinq_amnt, mths_since_last_delinq) |
| 3 | `defaulters_public_records` | Flags members with public record / enquiry risk (pub_rec, bankruptcies, inq_last_6mths) |

---

## Project Structure

```
lendingclub-project/
│
├── application_main.py          # Pipeline entry point
│
├── lib/
│   ├── __init__.py
│   ├── ConfigReader.py          # INI config loader with env validation
│   ├── DataReader.py            # StructType schemas + CSV reader functions
│   ├── DataManipulation.py      # All transformation functions — fully implemented
│   ├── Utils.py                 # Environment-aware SparkSession factory
│   └── logger.py                # Log4j wrapper
│
├── configs/
│   ├── application.conf         # File paths per environment (LOCAL / TEST / PROD)
│   └── pyspark.conf             # Spark tuning settings per environment
│
├── data/
│   ├── raw/                     # Source CSV files — not committed (see .gitignore)
│   └── cleaned/                 # Parquet output — generated at runtime
│
├── log4j.properties             # Log4j config — suppresses Spark noise, shows app logs
├── Pipfile                      # Python + dev dependencies (PySpark 3.5.0, pytest, chispa)
├── .gitignore
└── README.md
```

---

## Setup

### Prerequisites
- Python 3.11
- Java 8 or 11 (required by PySpark)
- `pipenv`

### Install
```bash
pipenv install --dev
pipenv shell
```

### Download the Dataset
Download the LendingClub dataset from [Kaggle](https://www.kaggle.com/datasets/wordsforthewise/lending-club) and place the four CSV files in `data/raw/`:
```
data/raw/customers.csv
data/raw/loans.csv
data/raw/loan_repayments.csv
data/raw/loan_defaulters.csv
```

### Run
```bash
# Local development
spark-submit application_main.py LOCAL

# Test environment
spark-submit application_main.py TEST

# Production (cluster with Hive)
spark-submit application_main.py PROD
```

---

## Design Decisions

**Explicit StructType schemas over `inferSchema=True`.** `inferSchema` scans the entire dataset on every run to guess types — slow on large files and unreliable on messy CSVs (a column with mostly integers but one `"N/A"` gets typed as string). Explicit schemas are defined once, fail fast if source structure changes, and cost zero at runtime.

**Pure transformation functions with `.transform()`.** Every function in `DataManipulation.py` takes a DataFrame and returns a DataFrame. No global state, no side effects. This makes each step independently unit-testable and the pipeline chain in `application_main.py` readable as a data flow diagram.

**Delinquency and public records split into two outputs.** Rather than flagging defaulters with a single boolean, the pipeline produces two distinct sub-datasets — one for delinquency signals and one for public record signals. These serve different downstream consumers: collections teams care about delinquency history, underwriting teams care about public records. Merging them into one table would force both consumers to filter every time.

**Grade-grouped `emp_length` imputation over global mean.** A global mean `emp_length` imputation loses the signal that borrowers in grade A (prime) tend to be more established employees than grade G (subprime). Imputing within grade group preserves this relationship, producing cleaner features for any downstream credit scoring work.

**Fail-fast config validation.** `ConfigReader` raises `ValueError` at startup if the requested environment section is missing from either config file. Without this guard, the pipeline would crash 10 steps in with a cryptic `KeyError` that takes time to trace back to a config issue.

---

## Tech Stack

| Tool | Role |
|---|---|
| Python 3.11 | Pipeline logic |
| Apache PySpark 3.5.0 | Distributed data processing |
| Log4j (via PySpark JVM bridge) | Structured logging |
| configparser | INI-based environment config |
| chispa | PySpark DataFrame unit testing |
| pipenv | Dependency and virtual environment management |

---

## Output Schema

All cleaned datasets are written as **Parquet** (columnar, compressed with Snappy in PROD) to `data/cleaned/`. Parquet is preferred over CSV for output because it preserves types, is 3-5x smaller on disk, and reads 10x faster in downstream Spark jobs.

Each output dataset includes an `ingest_date` timestamp column tracking when the record entered the pipeline.

---

## Author

**[Your Name]**  
[LinkedIn](https://linkedin.com/in/yourprofile) · [Portfolio](https://yourwebsite.com)
