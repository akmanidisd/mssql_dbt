# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Analytics Engineering project migrating MSSQL data to Snowflake and building dbt models. The project creates a metadata-driven approach to generate dbt staging models from MSSQL tables ingested into Snowflake.

**Role Context**: You are an Analytics Engineering Consultant continuing work on creating dbt staging and intermediate models following best practices.

**Security Note**: Work is done against a Snowflake database `MS_RAW` containing empty table structures and MSSQL metadata. For actual data from `ROL_RAW`, provide SQL statements to receive results.

## Repository Structure

### Core Databases

- **MS_RAW**: Snowflake database containing:
  - Data schemas: `DBO`, `DUPLICATEJOBSERVICE`, `JOBIMPORT`, `JOBS`, `RECRUITERJOBSTEXTKERNEL`
  - `MS_META` schema: MSSQL INFORMATION_SCHEMA copies (COLUMNS, KEY_COLUMN_USAGE, REFERENTIAL_CONSTRAINTS, TABLES, TABLE_CONSTRAINTS)
  - `DBT_META` schema: Metadata tables for dbt model generation

- **ROL_RAW**: Original Snowflake raw database (access restricted - request SQL results as needed)

### Directory Organization

- **sql_scripts/**: Numbered SQL migration scripts organized by task
  - `01_initial_setup/`: MS_RAW database and schema setup
  - `02_sf_ms_metadata/`: Snowflake-MSSQL metadata joining (SF_TABLES, SF_COLUMNS)
  - `03_normalising_timestamps/`: Date/timestamp column standardization
  - `04_dlt_load_id/`: dlt load tracking
  - `05_row_iteration/`: Row-level processing
  - `06_bitwise/`: Bitwise operations
  - `07_pk_fk_keys/`: Primary/Foreign key metadata and surrogate key logic
  - `10_dbt_staging/`: dbt staging model generation scripts

- **dbt_rol_prod/**: dbt project directory
  - `models/staging/reedonline/`: Staging models organized by source schema

## Architecture & Data Flow

### 1. Schema Name Mapping

MSSQL source schemas in ROL_RAW use `REEDONLINE_` prefix, which is stripped in MS_RAW:
- `ROL_RAW.REEDONLINE_DBO` → `MS_RAW.DBO`
- `ROL_RAW.REEDONLINE_JOBS` → `MS_RAW.JOBS`
- `ROL_RAW.REEDONLINE_RESTRICTED` → `MS_RAW.DBO` (renamed)

### 2. Metadata Integration

The project uses **standardized keys** for joining metadata across systems:

- `SCHEMA_ONLY_KEY`: Uppercase schema name
- `TABLE_ONLY_KEY`: Uppercase table name with underscores removed
- `COLUMN_ONLY_KEY`: Uppercase column name with underscores removed
- `TABLE_KEY`: `{SCHEMA_ONLY_KEY}.{TABLE_ONLY_KEY}`
- `COLUMN_KEY`: `{SCHEMA_ONLY_KEY}.{TABLE_ONLY_KEY}.{COLUMN_ONLY_KEY}` (primary identifier)

**Key Metadata Tables**:

- `MS_RAW.DBT_META.SF_TABLES`: Joined Snowflake + MSSQL table metadata
- `MS_RAW.DBT_META.SF_COLUMNS`: Joined Snowflake + MSSQL column metadata
- `MS_RAW.DBT_META.META_COLUMNS`: Comprehensive metadata table driving dbt staging generation

### 3. META_COLUMNS Table Structure

This is the **central metadata table** for dbt model generation, containing:

**Section 1: Identity & Keys** - Standardized keys for uniqueness
**Section 2: MSSQL Source Metadata** - Original MSSQL column definitions (MS_*)
**Section 3: Snowflake RAW Metadata** - Snowflake raw layer definitions (SF_RAW_*)
**Section 4: dbt Staging Definition** - Target staging model specifications (DBT_STG_*)
  - `DBT_STG_COLUMN_NAME`: Target column name (NULL to exclude)
  - `DBT_STG_DATA_TYPE`: Target data type
  - `DBT_STG_TRANSFORMATION`: SQL logic for transformation
  - `DBT_STG_IS_EXCLUDED`: Flag to exclude from staging
**Section 5: Primary Key Metadata** - PK constraints and surrogate key flags
**Section 6: Foreign Key Metadata** - FK relationships and parent references
**Section 7: Source Tracking** - Column source ('MSSQL', 'DERIVED', 'SURROGATE', 'BUSINESS')

### 4. Surrogate Key Strategy

**Composite Primary Keys**:
- Tables with multi-column PKs get a generated surrogate key: `{table_name}_id`
- Transformation uses dbt_utils: `{{ dbt_utils.generate_surrogate_key([...]) }}`
- Original PK columns remain but are no longer marked as PK in metadata

**Composite Foreign Keys**:
- Child tables with 2-column FKs to composite PK tables get a new surrogate FK column
- Original FK columns remain but FK metadata is cleared
- Surrogate FK column references the parent's surrogate PK

### 5. Column Name Standardization

**Timestamp/Date Columns**:
- Columns with patterns `DATE_*`, `*_DATE_*`, `*_DATE` are normalized
- `DATE_OF_BIRTH` → `BIRTH_DATE`
- Date parts removed from column names where appropriate
- Type standardization: MSSQL datetime → Snowflake DATE or TIMESTAMP_NTZ

## Working with Metadata

### Querying META_COLUMNS for a Table

```sql
SELECT
    TABLE_KEY,
    DBT_STG_COLUMN_NAME,
    DBT_STG_DATA_TYPE,
    IS_PRIMARY_KEY,
    IS_FOREIGN_KEY,
    FK_PARENT_FULL_KEY,
    DBT_STG_TRANSFORMATION,
    COLUMN_SOURCE,
    IS_NEW_COLUMN
FROM MS_RAW.DBT_META.META_COLUMNS
WHERE TABLE_KEY = 'DBO.TABLENAME'
  AND DBT_STG_IS_EXCLUDED = FALSE
ORDER BY DBT_STG_ORDINAL_POSITION;
```

### Adding Business Logic Columns

```sql
INSERT INTO MS_RAW.DBT_META.META_COLUMNS (
    SCHEMA_ONLY_KEY, TABLE_ONLY_KEY, COLUMN_ONLY_KEY,
    TABLE_KEY, COLUMN_KEY,
    DBT_STG_COLUMN_NAME, DBT_STG_DATA_TYPE,
    DBT_STG_TRANSFORMATION,
    COLUMN_SOURCE, IS_NEW_COLUMN,
    CREATED_BY
) VALUES (
    'DBO', 'EMPLOYEES', 'FULLNAME',
    'DBO.EMPLOYEES', 'DBO.EMPLOYEES.FULLNAME',
    'full_name', 'VARCHAR(500)',
    'CONCAT(first_name, '' '', last_name)',
    'BUSINESS', TRUE,
    CURRENT_USER()
);
```

### Excluding PII or Deprecated Columns

```sql
UPDATE MS_RAW.DBT_META.META_COLUMNS
SET DBT_STG_IS_EXCLUDED = TRUE,
    NOTES = 'PII field - excluded from staging'
WHERE COLUMN_KEY = 'DBO.EMPLOYEES.SSN';
```

## dbt Development

### Staging Model Conventions

- Models follow dbt best practices
- Source schemas: Use MSSQL schema names (DBO, JOBS, etc.)
- Model naming: `stg_reedonline__{schema}__{table}.sql`
- Column selection based on `META_COLUMNS.DBT_STG_IS_EXCLUDED = FALSE`
- Apply transformations from `DBT_STG_TRANSFORMATION`
- Surrogate keys use dbt_utils package

### Generating dbt Models

The metadata in `META_COLUMNS` should drive dbt model generation:
1. Query `META_COLUMNS` for table columns where `DBT_STG_IS_EXCLUDED = FALSE`
2. Order by `DBT_STG_ORDINAL_POSITION`
3. Apply `DBT_STG_TRANSFORMATION` if specified, otherwise use raw column
4. Include PK and FK tests based on metadata flags
5. Add surrogate key generation for `IS_SURROGATE_KEY = TRUE` columns

## Development Workflow

1. **Explore Metadata**: Query MS_RAW.DBT_META tables to understand table structures
2. **Review SQL Scripts**: Check `sql_scripts/` for context on metadata transformations
3. **Generate Staging Models**: Use META_COLUMNS to create dbt staging models
4. **Test**: Validate PK uniqueness, FK relationships, and data types
5. **Document**: Add column descriptions from `DBT_STG_DESCRIPTION` field

## Important Notes

- **Excluded Table**: `PRICEBOOK_ENTRY` (PRICEBOOKENTRY) is excluded from all metadata queries
- **Key Standardization**: All keys use UPPER case with underscores removed for matching
- **Schema Mapping**: Always map between REEDONLINE_* (source) and MS_RAW schemas
- **Metadata First**: Always consult META_COLUMNS before writing staging models
- **Surrogate Keys**: Single-column PKs/FKs are preserved; multi-column keys use surrogates
