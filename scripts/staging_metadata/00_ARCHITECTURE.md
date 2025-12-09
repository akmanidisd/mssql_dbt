# Staging Metadata Pipeline Architecture

## Overview

This directory contains a refactored, production-ready metadata pipeline using Snowflake stored procedures. The pipeline transforms MSSQL metadata from ROL_RAW into comprehensive dbt staging metadata tables.

## Design Principles

1. **Modular Stored Procedures**: Each major transformation is encapsulated in a reusable stored procedure
2. **Idempotent Operations**: All procedures can be run multiple times safely
3. **Error Handling**: Comprehensive exception handling with detailed logging
4. **Performance Optimized**: Uses Snowflake best practices (TRANSIENT tables, clustering, views)
5. **Single Execution**: One orchestrator procedure runs the entire pipeline
6. **Observability**: Audit logging table tracks execution history

## Architecture

### Final Output Tables

**1. STAGING_META_TABLES**
- Purpose: Table-level metadata for dbt model generation
- Schema: MS_RAW.DBT_META
- Contains: Table names, row counts, PK strategy, relationship counts, table classification

**2. STAGING_META_COLUMNS**
- Purpose: Column-level metadata for dbt model generation
- Schema: MS_RAW.DBT_META
- Contains: All information needed to generate dbt staging models
  - Source column definitions (MSSQL + Snowflake)
  - Target staging column definitions (name, type, transformation)
  - PK/FK relationships and surrogate key strategy
  - Column classification and business logic
  - Data quality rules and descriptions

### Pipeline Stages

```
┌─────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATOR PROCEDURE                       │
│                 SP_BUILD_STAGING_METADATA()                      │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ├──► Stage 1: Foundation Setup
                       │    └─ SP_SETUP_FOUNDATION()
                       │       ├─ Create schemas and base tables
                       │       ├─ Copy MSSQL metadata to MS_META
                       │       └─ Create utility views and UDFs
                       │
                       ├──► Stage 2: Metadata Integration
                       │    └─ SP_JOIN_METADATA()
                       │       ├─ Create SF_TABLES (SF + MS joined)
                       │       ├─ Create SF_COLUMNS (SF + MS joined)
                       │       └─ Create V_INGESTED_TABLES view
                       │
                       ├──► Stage 3: Column Transformations
                       │    └─ SP_NORMALIZE_COLUMNS()
                       │       ├─ Timestamp/date normalization
                       │       ├─ Data type conversions
                       │       ├─ Column naming conventions
                       │       └─ Derived column definitions
                       │
                       ├──► Stage 4: Relationship Analysis
                       │    └─ SP_ANALYZE_RELATIONSHIPS()
                       │       ├─ Extract single-column PKs
                       │       ├─ Identify composite PK tables
                       │       ├─ Analyze FK relationships
                       │       └─ Plan surrogate key strategy
                       │
                       ├──► Stage 5: Build Final Tables
                       │    └─ SP_BUILD_FINAL_METADATA()
                       │       ├─ Create STAGING_META_COLUMNS
                       │       │  ├─ Load base columns from SF_COLUMNS
                       │       │  ├─ Apply transformations
                       │       │  ├─ Add PK/FK metadata
                       │       │  ├─ Insert surrogate keys
                       │       │  └─ Add derived columns
                       │       └─ Create STAGING_META_TABLES
                       │          ├─ Aggregate column metadata
                       │          ├─ Calculate table statistics
                       │          └─ Classify table types
                       │
                       └──► Stage 6: Validation & Logging
                            └─ SP_VALIDATE_METADATA()
                               ├─ Run data quality checks
                               ├─ Validate referential integrity
                               └─ Log execution results
```

## Stored Procedures

### 1. SP_SETUP_FOUNDATION()
**Purpose**: Initialize database structures and utilities
**Creates**:
- MS_RAW database and data schemas (DBO, JOBS, etc.)
- MS_RAW.MS_META schema with MSSQL metadata copies
- MS_RAW.DBT_META schema for transformation metadata
- UDF: `UDF_GENERATE_COLUMN_KEY(schema, table, column)` - Standardized key generation
- View: `V_SCHEMA_MAPPING` - ROL_RAW to MS_RAW schema mapping
- Table: `METADATA_BUILD_LOG` - Audit trail for pipeline executions

**Returns**: VARCHAR (success/error message)

### 2. SP_JOIN_METADATA()
**Purpose**: Join Snowflake and MSSQL metadata
**Creates**:
- `SF_TABLES` - Table metadata joined by TABLE_KEY
- `SF_COLUMNS` - Column metadata joined by COLUMN_KEY
- `V_INGESTED_TABLES` - Filtered view of ingested tables (excludes PRICEBOOKENTRY)

**Parameters**:
- `EXCLUDE_TABLES` VARCHAR (default: 'PRICEBOOKENTRY') - Comma-separated list of tables to exclude

**Returns**: VARCHAR (row counts created)

### 3. SP_NORMALIZE_COLUMNS()
**Purpose**: Apply column transformations and business rules
**Creates**:
- `COLUMN_TRANSFORMATIONS` - Transformation rules for each column
  - Timestamp/date normalization (DATE_OF_BIRTH → BIRTH_DATE)
  - Type conversions (datetime → DATE/TIMESTAMP_NTZ)
  - Naming conventions (_ON → _AT, append _DATE)
  - Derived columns (dlt_load_id → _dlt_loaded_at)
  - Row iteration columns (time_stamp → row_iteration_num)

**Logic**:
1. Analyze TIMESTAMP_TZ columns to detect date-only values
2. Apply naming convention rules based on data type
3. Handle special cases (BIRTH, HISTORY_FROM, etc.)
4. Generate transformation SQL for each column

**Returns**: VARCHAR (transformation count)

### 4. SP_ANALYZE_RELATIONSHIPS()
**Purpose**: Analyze PK/FK relationships and plan surrogate keys
**Creates**:
- `V_SINGLE_COLUMN_PKS` - Tables with single-column primary keys
- `V_COMPOSITE_PKS` - Tables needing surrogate keys (composite PK or no PK)
- `V_FK_RELATIONSHIPS` - All foreign key relationships
- `V_SURROGATE_KEY_PLAN` - Complete surrogate key implementation plan
  - Parent tables: surrogate PK columns to add
  - Child tables: surrogate FK columns to add
  - Original FK columns to preserve (FK metadata cleared)

**Returns**: VARCHAR (relationship statistics)

### 5. SP_BUILD_FINAL_METADATA()
**Purpose**: Build STAGING_META_COLUMNS and STAGING_META_TABLES
**Creates**:
- `STAGING_META_COLUMNS` - Final column-level metadata (all columns needed for dbt)
- `STAGING_META_TABLES` - Final table-level metadata (aggregated statistics)

**Logic**:
1. Load base columns from SF_COLUMNS
2. Merge COLUMN_TRANSFORMATIONS (apply naming/type changes)
3. Merge PK metadata (single-column PKs)
4. Merge FK metadata (single-column FKs)
5. Insert surrogate PK columns (for composite PK tables)
6. Insert surrogate FK columns (for child tables)
7. Clear original composite FK metadata
8. Insert derived columns (dlt, row_iteration, business logic)
9. Aggregate to STAGING_META_TABLES

**Returns**: VARCHAR (table/column counts)

### 6. SP_VALIDATE_METADATA()
**Purpose**: Run data quality checks and validation
**Checks**:
- All ingested tables have entries in STAGING_META_TABLES
- All tables have at least one PK column
- All FK parent references exist in STAGING_META_COLUMNS
- No duplicate COLUMN_KEYs
- All non-excluded columns have DBT_STG_COLUMN_NAME
- Surrogate key naming follows convention

**Returns**: VARCHAR (validation results summary)

### 7. SP_BUILD_STAGING_METADATA() - ORCHESTRATOR
**Purpose**: Execute complete pipeline in correct order
**Parameters**:
- `REBUILD_FOUNDATION` BOOLEAN (default: FALSE) - If TRUE, recreates base structures
- `EXCLUDE_TABLES` VARCHAR (default: 'PRICEBOOKENTRY')
- `VALIDATE_RESULTS` BOOLEAN (default: TRUE)

**Execution Flow**:
1. Log start time
2. IF REBUILD_FOUNDATION: Call SP_SETUP_FOUNDATION()
3. Call SP_JOIN_METADATA(EXCLUDE_TABLES)
4. Call SP_NORMALIZE_COLUMNS()
5. Call SP_ANALYZE_RELATIONSHIPS()
6. Call SP_BUILD_FINAL_METADATA()
7. IF VALIDATE_RESULTS: Call SP_VALIDATE_METADATA()
8. Log completion and return summary

**Returns**: VARCHAR (execution summary with timing)

## STAGING_META_COLUMNS Schema

```sql
CREATE OR REPLACE TABLE MS_RAW.DBT_META.STAGING_META_COLUMNS (
    -- Identity & Keys
    COLUMN_KEY              VARCHAR PRIMARY KEY,
    SCHEMA_KEY              VARCHAR NOT NULL,
    TABLE_KEY               VARCHAR NOT NULL,
    COLUMN_NAME_KEY         VARCHAR NOT NULL,

    -- Source Metadata (MSSQL)
    MS_SCHEMA               VARCHAR,
    MS_TABLE                VARCHAR,
    MS_COLUMN               VARCHAR,
    MS_DATA_TYPE            VARCHAR,
    MS_IS_NULLABLE          BOOLEAN,
    MS_MAX_LENGTH           NUMBER,
    MS_PRECISION            NUMBER,
    MS_SCALE                NUMBER,
    MS_DEFAULT_VALUE        VARCHAR,

    -- Source Metadata (Snowflake RAW)
    SF_SCHEMA               VARCHAR NOT NULL,
    SF_TABLE                VARCHAR NOT NULL,
    SF_COLUMN               VARCHAR NOT NULL,
    SF_DATA_TYPE            VARCHAR NOT NULL,
    SF_IS_NULLABLE          BOOLEAN,
    SF_MAX_LENGTH           NUMBER,
    SF_PRECISION            NUMBER,
    SF_SCALE                NUMBER,
    SF_ORDINAL_POSITION     NUMBER,

    -- Target Staging Definition
    DBT_COLUMN_NAME         VARCHAR,            -- NULL = excluded from staging
    DBT_DATA_TYPE           VARCHAR,
    DBT_IS_NULLABLE         BOOLEAN,
    DBT_TRANSFORMATION      VARCHAR,            -- SQL transformation logic
    DBT_DESCRIPTION         VARCHAR,
    DBT_ORDINAL_POSITION    NUMBER,
    DBT_IS_EXCLUDED         BOOLEAN DEFAULT FALSE,

    -- Primary Key Metadata
    IS_PRIMARY_KEY          BOOLEAN DEFAULT FALSE,
    IS_SURROGATE_KEY        BOOLEAN DEFAULT FALSE,
    PK_CONSTRAINT_NAME      VARCHAR,
    PK_ORDINAL              NUMBER,
    PK_COLUMNS_ARRAY        ARRAY,              -- For composite PKs (original columns)

    -- Foreign Key Metadata
    IS_FOREIGN_KEY          BOOLEAN DEFAULT FALSE,
    FK_CONSTRAINT_NAME      VARCHAR,
    FK_PARENT_SCHEMA        VARCHAR,
    FK_PARENT_TABLE         VARCHAR,
    FK_PARENT_COLUMN        VARCHAR,
    FK_PARENT_KEY           VARCHAR,            -- Full parent COLUMN_KEY
    FK_ORDINAL              NUMBER,

    -- Column Classification
    COLUMN_SOURCE           VARCHAR(20),        -- MSSQL, DERIVED, SURROGATE, BUSINESS, SYSTEM
    COLUMN_CATEGORY         VARCHAR(50),        -- ID, TIMESTAMP, DATE, FLAG, MEASURE, DIMENSION, etc.
    IS_NEW_COLUMN           BOOLEAN DEFAULT FALSE,
    IS_COMPUTED             BOOLEAN DEFAULT FALSE,

    -- Data Quality
    IS_REQUIRED             BOOLEAN,            -- Business logic: must not be NULL
    DQ_RULE                 VARCHAR,            -- Custom validation rule

    -- Metadata
    NOTES                   VARCHAR,
    TAGS                    ARRAY,
    CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_NTZ,
    CREATED_BY              VARCHAR,
    UPDATED_BY              VARCHAR
)
CLUSTER BY (TABLE_KEY, DBT_ORDINAL_POSITION);
```

## STAGING_META_TABLES Schema

```sql
CREATE OR REPLACE TABLE MS_RAW.DBT_META.STAGING_META_TABLES (
    -- Identity
    TABLE_KEY               VARCHAR PRIMARY KEY,
    SCHEMA_KEY              VARCHAR NOT NULL,
    TABLE_NAME_KEY          VARCHAR NOT NULL,

    -- Source Metadata
    MS_SCHEMA               VARCHAR,
    MS_TABLE                VARCHAR,
    SF_SCHEMA               VARCHAR NOT NULL,
    SF_TABLE                VARCHAR NOT NULL,
    DBT_MODEL_NAME          VARCHAR,            -- stg_reedonline__schema__table

    -- Table Classification
    TABLE_TYPE              VARCHAR(50),        -- FACT, DIMENSION, BRIDGE, REFERENCE, SYSTEM
    HAS_COMPOSITE_PK        BOOLEAN DEFAULT FALSE,
    HAS_SURROGATE_PK        BOOLEAN DEFAULT FALSE,
    HAS_NATURAL_PK          BOOLEAN DEFAULT FALSE,
    HAS_NO_PK               BOOLEAN DEFAULT FALSE,

    -- Column Statistics
    TOTAL_COLUMNS           NUMBER,
    SOURCE_COLUMNS          NUMBER,             -- From MSSQL
    DERIVED_COLUMNS         NUMBER,             -- dlt, row_iteration, business logic
    SURROGATE_COLUMNS       NUMBER,             -- Surrogate keys
    EXCLUDED_COLUMNS        NUMBER,

    -- Key Statistics
    PK_COLUMN_COUNT         NUMBER,
    ORIGINAL_PK_COLUMNS     ARRAY,              -- Original PK column names
    SURROGATE_PK_NAME       VARCHAR,            -- e.g., "table_name_id"
    FK_COUNT                NUMBER,             -- Outgoing FKs
    CHILD_TABLE_COUNT       NUMBER,             -- Incoming FKs (how many tables reference this)

    -- Data Type Summary
    TIMESTAMP_COLUMNS       NUMBER,
    DATE_COLUMNS            NUMBER,
    FLAG_COLUMNS            NUMBER,             -- BOOLEAN
    ID_COLUMNS              NUMBER,
    TEXT_COLUMNS            NUMBER,
    NUMERIC_COLUMNS         NUMBER,

    -- Metadata
    NOTES                   VARCHAR,
    TAGS                    ARRAY,
    DBT_MATERIALIZATION     VARCHAR DEFAULT 'view',  -- view, table, incremental
    ROW_COUNT_ESTIMATE      NUMBER,             -- From Snowflake stats (if available)
    CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_NTZ
)
CLUSTER BY (SCHEMA_KEY, TABLE_NAME_KEY);
```

## Utility Views

### V_INGESTED_TABLES
```sql
-- Filtered list of tables to process (excludes PRICEBOOKENTRY, system tables)
```

### V_SINGLE_COLUMN_PKS
```sql
-- Tables with simple single-column primary keys
```

### V_COMPOSITE_PKS
```sql
-- Tables needing surrogate keys (composite PK or no PK)
```

### V_FK_RELATIONSHIPS
```sql
-- All FK relationships with parent/child metadata
```

### V_SURROGATE_KEY_PLAN
```sql
-- Complete plan for surrogate key implementation
```

### V_COLUMN_TRANSFORMATIONS
```sql
-- All column transformation rules ready to apply
```

## UDFs

### UDF_GENERATE_COLUMN_KEY(schema VARCHAR, table VARCHAR, column VARCHAR)
Returns: VARCHAR
```sql
-- Returns: UPPER(REPLACE(schema || '.' || table || '.' || column, '_', ''))
```

### UDF_GENERATE_SURROGATE_KEY_NAME(table_name VARCHAR)
Returns: VARCHAR
```sql
-- Returns: LOWER(table_name) || '_id'
```

### UDF_CLASSIFY_COLUMN_CATEGORY(column_name VARCHAR, data_type VARCHAR)
Returns: VARCHAR
```sql
-- Returns: ID, TIMESTAMP, DATE, FLAG, MEASURE, DIMENSION, TEXT, etc.
-- Based on naming patterns and data type
```

## Execution

### Full Rebuild
```sql
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(
    REBUILD_FOUNDATION => TRUE,
    EXCLUDE_TABLES => 'PRICEBOOKENTRY',
    VALIDATE_RESULTS => TRUE
);
```

### Incremental Update (metadata already exists)
```sql
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(
    REBUILD_FOUNDATION => FALSE,
    EXCLUDE_TABLES => 'PRICEBOOKENTRY',
    VALIDATE_RESULTS => TRUE
);
```

### Check Execution History
```sql
SELECT *
FROM MS_RAW.DBT_META.METADATA_BUILD_LOG
ORDER BY EXECUTION_START_TIME DESC
LIMIT 10;
```

## Benefits Over Original Scripts

1. **Single Execution**: One procedure call vs. 17+ script files
2. **Error Handling**: Automatic rollback on failure with detailed error messages
3. **Idempotent**: Can rerun safely without side effects
4. **Modular**: Each stage can be run independently for debugging
5. **Observable**: Execution logging and validation built-in
6. **Performance**: Optimized with clustered tables and views
7. **Maintainable**: Clear separation of concerns, easier to modify
8. **Type Safety**: Parameter validation and consistent data types
9. **Documentation**: Self-documenting with procedure comments
10. **Version Control**: All logic in procedures, no ad-hoc script execution

## File Structure

```
scripts/staging_metadata/
├── 00_ARCHITECTURE.md                          (this file)
├── 01_foundation.sql                           (SP_SETUP_FOUNDATION)
├── 02_metadata_integration.sql                 (SP_JOIN_METADATA)
├── 03_column_transformations.sql               (SP_NORMALIZE_COLUMNS)
├── 04_relationship_analysis.sql                (SP_ANALYZE_RELATIONSHIPS)
├── 05_build_final_metadata.sql                 (SP_BUILD_FINAL_METADATA)
├── 06_validation.sql                           (SP_VALIDATE_METADATA)
├── 07_orchestrator.sql                         (SP_BUILD_STAGING_METADATA)
├── 99_deploy_all.sql                           (Deploy all procedures in order)
└── README.md                                   (Quick start guide)
```

## Migration from Old Scripts

The old sql_scripts/ directory is preserved for reference. To migrate:

1. Deploy all stored procedures: `99_deploy_all.sql`
2. Run orchestrator: `CALL SP_BUILD_STAGING_METADATA(TRUE, 'PRICEBOOKENTRY', TRUE)`
3. Verify results: Query STAGING_META_COLUMNS and STAGING_META_TABLES
4. Update dbt model generation to use new tables

## Future Enhancements

1. **Incremental Updates**: Detect schema changes and update metadata incrementally
2. **Business Rules Engine**: Store and apply business rules via configuration table
3. **Data Profiling**: Analyze actual data to suggest transformations and detect quality issues
4. **Impact Analysis**: Track downstream dependencies when metadata changes
5. **API Integration**: Expose metadata via Snowflake stored procedures as REST API
