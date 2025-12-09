# Quick Reference Card

## One-Line Deployment & Execution

```sql
-- Deploy all procedures
!source scripts/staging_metadata/99_deploy_all.sql

-- Execute pipeline (first time)
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(TRUE, 'PRICEBOOKENTRY', TRUE);

-- Execute pipeline (subsequent runs)
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(FALSE, 'PRICEBOOKENTRY', TRUE);
```

## Essential Queries

### View All Tables
```sql
SELECT TABLE_KEY, DBT_MODEL_NAME, TOTAL_COLUMNS, PK_COLUMN_COUNT, FK_COUNT
FROM MS_RAW.DBT_META.STAGING_META_TABLES
ORDER BY SCHEMA_KEY, SF_TABLE;
```

### View Columns for a Table
```sql
SELECT DBT_COLUMN_NAME, DBT_DATA_TYPE, IS_PRIMARY_KEY, IS_FOREIGN_KEY
FROM MS_RAW.DBT_META.STAGING_META_COLUMNS
WHERE TABLE_KEY = 'DBO.EMPLOYEES' AND NOT DBT_IS_EXCLUDED
ORDER BY DBT_ORDINAL_POSITION;
```

### Check Last Execution
```sql
SELECT LOG_ID, EXECUTION_START_TIME, DURATION_SECONDS, STATUS, TABLES_CREATED, COLUMNS_CREATED
FROM MS_RAW.DBT_META.METADATA_BUILD_LOG
ORDER BY EXECUTION_START_TIME DESC LIMIT 1;
```

### View Surrogate Key Plan
```sql
SELECT TABLE_KEY, NEW_COLUMN_NAME, IS_PRIMARY_KEY, IS_FOREIGN_KEY, NOTES
FROM MS_RAW.DBT_META.V_SURROGATE_KEY_PLAN
ORDER BY TABLE_KEY, ORDINAL_POSITION;
```

## Common Operations

### Exclude Additional Tables
```sql
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(
    FALSE,
    'PRICEBOOKENTRY,TEMP_TABLE,OLD_TABLE',
    TRUE
);
```

### Run Without Validation (Faster)
```sql
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(FALSE, 'PRICEBOOKENTRY', FALSE);
```

### Validate Manually
```sql
CALL MS_RAW.DBT_META.SP_VALIDATE_METADATA();
```

### Exclude PII Column
```sql
UPDATE MS_RAW.DBT_META.STAGING_META_COLUMNS
SET DBT_IS_EXCLUDED = TRUE, NOTES = 'PII'
WHERE COLUMN_KEY = 'DBO.EMPLOYEES.SSN';
```

### Add Business Logic Column
```sql
INSERT INTO MS_RAW.DBT_META.STAGING_META_COLUMNS (
    COLUMN_KEY, SCHEMA_KEY, TABLE_KEY, COLUMN_NAME_KEY,
    SF_SCHEMA, SF_TABLE, SF_COLUMN, SF_DATA_TYPE,
    DBT_COLUMN_NAME, DBT_DATA_TYPE, DBT_TRANSFORMATION,
    COLUMN_SOURCE, IS_NEW_COLUMN, CREATED_BY
) VALUES (
    'DBO.EMPLOYEES.FULLNAME', 'DBO', 'DBO.EMPLOYEES', 'FULLNAME',
    'DBO', 'EMPLOYEES', 'FULL_NAME', 'VARCHAR(500)',
    'full_name', 'VARCHAR(500)', 'CONCAT(first_name, '' '', last_name)',
    'BUSINESS', TRUE, CURRENT_USER()
);
```

## Stored Procedures

| Procedure | Purpose | Typical Duration |
|-----------|---------|------------------|
| `SP_SETUP_FOUNDATION` | Create database, schemas, UDFs | 10-30 sec |
| `SP_JOIN_METADATA` | Join SF + MS metadata | 30-60 sec |
| `SP_NORMALIZE_COLUMNS` | Apply transformations | 30-90 sec |
| `SP_ANALYZE_RELATIONSHIPS` | PK/FK analysis | 20-60 sec |
| `SP_BUILD_FINAL_METADATA` | Build final tables | 60-180 sec |
| `SP_VALIDATE_METADATA` | Run validation | 10-30 sec |
| `SP_BUILD_STAGING_METADATA` | **Orchestrator (runs all)** | **3-10 min** |

## Key Tables

| Table | Purpose | Typical Row Count |
|-------|---------|-------------------|
| `STAGING_META_TABLES` | Table-level metadata | 100-500 |
| `STAGING_META_COLUMNS` | Column-level metadata | 5,000-50,000 |
| `SF_TABLES` | SF + MS tables joined | 100-500 |
| `SF_COLUMNS` | SF + MS columns joined | 5,000-50,000 |
| `METADATA_BUILD_LOG` | Execution audit trail | 1 per execution |

## Key Views

| View | Purpose |
|------|---------|
| `V_INGESTED_TABLES` | Filtered list of tables to process |
| `V_SINGLE_COLUMN_PKS` | Tables with simple PKs |
| `V_COMPOSITE_PKS` | Tables needing surrogate keys |
| `V_FK_RELATIONSHIPS` | All FK relationships |
| `V_SURROGATE_KEY_PLAN` | Surrogate key implementation plan |

## Troubleshooting

### Pipeline Fails
```sql
-- Check error in log
SELECT ERROR_MESSAGE FROM MS_RAW.DBT_META.METADATA_BUILD_LOG
WHERE STATUS = 'FAILED' ORDER BY EXECUTION_START_TIME DESC LIMIT 1;
```

### Validation Fails
```sql
-- Run validation to see errors
CALL MS_RAW.DBT_META.SP_VALIDATE_METADATA();
```

### Performance Issues
```sql
-- Increase warehouse size
ALTER WAREHOUSE <warehouse_name> SET WAREHOUSE_SIZE = 'LARGE';
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(...);
ALTER WAREHOUSE <warehouse_name> SET WAREHOUSE_SIZE = 'SMALL';
```

## File Locations

```
scripts/staging_metadata/
├── 00_ARCHITECTURE.md              (Detailed design)
├── 01_foundation.sql               (Setup procedure)
├── 02_metadata_integration.sql     (Integration procedure)
├── 03_column_transformations.sql   (Transformation procedure)
├── 04_relationship_analysis.sql    (Analysis procedure)
├── 05_build_final_metadata.sql     (Build procedure)
├── 06_validation.sql               (Validation procedure)
├── 07_orchestrator.sql             (Main orchestrator)
├── 99_deploy_all.sql               (Deploy script)
├── README.md                       (Getting started)
├── IMPLEMENTATION_SUMMARY.md       (Complete summary)
└── QUICK_REFERENCE.md              (This file)
```

## Parameters

### SP_BUILD_STAGING_METADATA Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_rebuild_foundation` | BOOLEAN | FALSE | Recreate base structures |
| `p_exclude_tables` | VARCHAR | 'PRICEBOOKENTRY' | Comma-separated table names |
| `p_validate_results` | BOOLEAN | TRUE | Run validation after build |

## Status Values

| Status | Meaning |
|--------|---------|
| `RUNNING` | Pipeline is currently executing |
| `SUCCESS` | Pipeline completed successfully |
| `FAILED` | Pipeline failed with errors |

## Column Categories

| Category | Description | Example Columns |
|----------|-------------|-----------------|
| `ID` | Identifier columns | employee_id, customer_id |
| `TIMESTAMP` | Timestamp columns | created_at, updated_at |
| `DATE` | Date columns | birth_date, hire_date |
| `FLAG` | Boolean flags | is_active, has_permissions |
| `MEASURE` | Numeric measures | amount, price, quantity |
| `NUMERIC` | Other numbers | age, count |
| `TEXT` | Text columns | name, description |
| `DIMENSION` | Other dimensions | status, type |

## Column Sources

| Source | Description | Examples |
|--------|-------------|----------|
| `MSSQL` | Original MSSQL columns | All source columns |
| `DERIVED` | Derived transformations | _dlt_loaded_at, row_iteration_num |
| `SURROGATE` | Generated surrogate keys | employee_id (from composite PK) |
| `BUSINESS` | Business logic columns | full_name, total_amount |

## Useful Links

- Architecture: `00_ARCHITECTURE.md`
- Getting Started: `README.md`
- Full Summary: `IMPLEMENTATION_SUMMARY.md`
- Deployment: `99_deploy_all.sql`
