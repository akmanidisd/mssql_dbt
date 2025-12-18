# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Snowflake metadata transformation project** that migrates Microsoft SQL Server (MSSQL) database metadata from ReedOnline to Snowflake, preparing it for dbt staging models. The project analyzes and transforms table/column structures, primary keys, foreign keys, and data types to create normalized staging metadata.

## Architecture

### Directory Structure

- **`scripts/staging_meta/`** - Sequential SQL scripts that build metadata transformation pipeline in Snowflake
- **`dbt_rol_prod/models/staging/reedonline/`** - dbt staging models (currently empty, target for generated models)

### Metadata Pipeline Flow

The pipeline executes in this specific order (numbered 11-18):

1. **11_foundation.sql** - Creates `MS_RAW.STG_META` schema
2. **12_tables.sql** - Maps MS tables to SF tables, applies table name transformations
3. **13_columns.sql** - Maps MS columns to SF columns, applies column name transformations
4. **14a_timestamp_column_analysis.sql** - Analyzes timestamp columns to identify date-only fields
5. **14b_timestamps_dates.sql** - Transforms timestamp columns (TIMESTAMP_TZ → TIMESTAMP_NTZ or DATE)
6. **15_dlt_load_id.sql** - Adds `_DLT_LOAD_ID_NUM` and `_DLT_LOAD_ID_AT` derived columns
7. **16_row_iteration.sql** - Adds row iteration metadata columns
8. **17_pk.sql** - Analyzes and transforms primary keys (handles 1 and 2-column PKs differently)
9. **18_fk.sql** - Analyzes foreign key relationships and creates dbt test metadata

### Key Metadata Tables

- **`MS_RAW.STG_META.SF_TABLES`** - Table mapping with renamed table names and PK metadata
- **`MS_RAW.STG_META.SF_COLUMNS`** - Column mapping with renamed columns and type transformations
- **`MS_RAW.STG_META.T_PRIMARY_KEYS`** - Primary key analysis (transient working table)
- **`MS_RAW.STG_META.T_SECONDARY_KEYS`** - Foreign key relationships (transient working table)
- **`MS_RAW.STG_META.T_TWO_KEYS_DBT_TEST`** - Foreign keys pointing to 2-column PKs

## Data Sources

The project reads from:
- **`ROL_RAW.INFORMATION_SCHEMA.*`** - Snowflake information schema for current table/column metadata
- **`ROL_RAW.REEDONLINE_META.*`** - MSSQL metadata tables (TABLES, COLUMNS, TABLE_CONSTRAINTS, KEY_COLUMN_USAGE, REFERENTIAL_CONSTRAINTS)

Target schemas:
- **`REEDONLINE_DBO`**
- **`REEDONLINE_DUPLICATEJOBSERVICE`**
- **`REEDONLINE_JOBIMPORT`**
- **`REEDONLINE_JOBS`**

## Naming Transformations

### Table Naming Rules

Tables undergo multiple transformations via nested REPLACE statements:
- Plurals to singular (e.g., `JOBS` → `JOB`, `USERS` → `USER`)
- Remove prefixes: `LOOKUP_`, `^LOOKUP_`
- Remove suffixes: `_FACT`, `_FACT_FK`, `_LOOKUP`, `_TYPES` → `_TYPE`
- Specific mappings (e.g., `CANDIDATETOOLSACTIONLOOKUP` → `CANDIDATE_TOOLS_ACTION`)
- Typo corrections (e.g., `ECRUITER` → `RECRUITER`, `RRECRUITER` → `RECRUITER`)

### Column Naming Rules

- Remove typos: `ECRUITER` → `RECRUITER`, `RRECRUITER` → `RECRUITER`
- Date standardization: `DATE_OF_BIRTH` → `BIRTH_DATE`
- ID suffix: `_BW$` → `_ID`
- Timestamp columns: Base name + `_AT` suffix (e.g., `CREATED_AT`)
- Date columns: Base name + `_DATE` suffix (e.g., `BIRTH_DATE`)

### Primary Key Rules

- **Single-column PKs**: Renamed to `{TABLE_NAME}_ID` when possible
- **Two-column PKs**: Composite key generated as `COL1 * 10^12 + COL2`, named `{TABLE_NAME}_ID`
- **Multi-table PKs** (PK_TABLES > 1): Identifies shared PKs across related tables

## Working with the Codebase

### Executing the Pipeline

The scripts must be run sequentially in numeric order (11 → 18) in Snowflake:

```bash
# Execute scripts in Snowflake SQL worksheet in this order:
# 11_foundation.sql
# 12_tables.sql
# 13_columns.sql
# 14a_timestamp_column_analysis.sql
# 14b_timestamps_dates.sql
# 15_dlt_load_id.sql
# 16_row_iteration.sql
# 17_pk.sql
# 18_fk.sql
```

Each script contains validation queries (e.g., `SELECT COUNT(*)`) to verify transformations.

### Python Environment

- Python version: **3.11.8** (managed via pyenv, see `.python-version`)
- Virtual environment: **`.venv/`** (excluded from git)
- dbt is NOT currently installed in the venv

### Important Constraints

- **PRICEBOOK_ENTRY table** is explicitly excluded (has 5-column PK, too complex)
- **DLT tables** (`_DLT%` prefix) are excluded from source tables
- All transformations preserve `SF_TABLE_SCHEMA` and `SF_TABLE_NAME` for traceability

## Key Patterns to Follow

1. **Transient Tables**: Working tables like `T_PRIMARY_KEYS` are transient and cleaned up after use
2. **Views**: Views like `V_PRIMARY_KEYS_TWO` provide intermediate transformation logic
3. **Validation Queries**: Each major transformation includes COUNT and sample queries
4. **Insert Safety**: Scripts check for existing records before INSERT to prevent duplicates
5. **Schema Qualification**: Always use full schema qualification (`MS_RAW.STG_META.TABLE_NAME`)

## Git Workflow

Current branch: `main` (also the main branch for PRs)

Recent activity focuses on primary key and foreign key metadata transformation (commits: "pk", "V20251216").
