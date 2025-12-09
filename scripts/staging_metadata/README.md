# Staging Metadata Pipeline

A production-ready, metadata-driven pipeline for generating dbt staging models from MSSQL tables ingested into Snowflake.

## Quick Start

### 1. Deploy All Procedures

```sql
-- From Snowflake SQL worksheet or SnowSQL CLI:
USE ROLE ACCOUNTADMIN;
!source scripts/staging_metadata/99_deploy_all.sql
```

Or deploy individually:

```bash
# Using SnowSQL
snowsql -a <account> -u <user> -f scripts/staging_metadata/01_foundation.sql
snowsql -a <account> -u <user> -f scripts/staging_metadata/02_metadata_integration.sql
# ... continue for all 7 procedures
```

### 2. Execute the Pipeline

**First Time (Full Build):**
```sql
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(
    p_rebuild_foundation => TRUE,
    p_exclude_tables => 'PRICEBOOKENTRY',
    p_validate_results => TRUE
);
```

**Incremental Update:**
```sql
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(
    p_rebuild_foundation => FALSE,
    p_exclude_tables => 'PRICEBOOKENTRY',
    p_validate_results => TRUE
);
```

### 3. Query the Results

**View Table Metadata:**
```sql
SELECT *
FROM MS_RAW.DBT_META.STAGING_META_TABLES
ORDER BY SCHEMA_KEY, TABLE_NAME_KEY;
```

**View Column Metadata for a Specific Table:**
```sql
SELECT
    DBT_COLUMN_NAME,
    DBT_DATA_TYPE,
    DBT_TRANSFORMATION,
    IS_PRIMARY_KEY,
    IS_FOREIGN_KEY,
    FK_PARENT_KEY,
    COLUMN_SOURCE,
    COLUMN_CATEGORY
FROM MS_RAW.DBT_META.STAGING_META_COLUMNS
WHERE TABLE_KEY = 'DBO.EMPLOYEES'
  AND DBT_IS_EXCLUDED = FALSE
ORDER BY DBT_ORDINAL_POSITION;
```

**Check Execution History:**
```sql
SELECT
    LOG_ID,
    EXECUTION_START_TIME,
    DURATION_SECONDS,
    STATUS,
    VALIDATION_PASSED,
    TABLES_CREATED,
    COLUMNS_CREATED,
    ERROR_MESSAGE
FROM MS_RAW.DBT_META.METADATA_BUILD_LOG
ORDER BY EXECUTION_START_TIME DESC
LIMIT 10;
```

## Architecture Overview

The pipeline consists of 6 stages orchestrated by one main procedure:

```
SP_BUILD_STAGING_METADATA (Orchestrator)
├── Stage 1: SP_SETUP_FOUNDATION           (Optional - first run only)
├── Stage 2: SP_JOIN_METADATA              (Join SF + MS metadata)
├── Stage 3: SP_NORMALIZE_COLUMNS          (Apply transformations)
├── Stage 4: SP_ANALYZE_RELATIONSHIPS      (PK/FK + surrogate keys)
├── Stage 5: SP_BUILD_FINAL_METADATA       (Build final tables)
└── Stage 6: SP_VALIDATE_METADATA          (Optional - data quality checks)
```

## Output Tables

### STAGING_META_TABLES
Table-level metadata for all ingested tables.

**Key Columns:**
- `TABLE_KEY`: Unique table identifier
- `DBT_MODEL_NAME`: Generated dbt model name (e.g., `stg_reedonline__dbo__employees`)
- `HAS_SURROGATE_PK`: Boolean - table uses generated surrogate key
- `PK_COLUMN_COUNT`, `FK_COUNT`: Relationship statistics
- `TOTAL_COLUMNS`, `SOURCE_COLUMNS`, `DERIVED_COLUMNS`: Column counts

### STAGING_META_COLUMNS
Column-level metadata for all columns in all tables.

**Key Columns:**
- `COLUMN_KEY`: Unique column identifier
- `DBT_COLUMN_NAME`: Target staging column name
- `DBT_DATA_TYPE`: Target data type
- `DBT_TRANSFORMATION`: SQL transformation logic (if applicable)
- `IS_PRIMARY_KEY`, `IS_FOREIGN_KEY`: Relationship flags
- `FK_PARENT_KEY`: References parent column for FKs
- `COLUMN_SOURCE`: 'MSSQL', 'DERIVED', 'SURROGATE', 'BUSINESS'
- `COLUMN_CATEGORY`: 'ID', 'TIMESTAMP', 'DATE', 'FLAG', 'MEASURE', etc.

## Common Tasks

### Exclude Additional Tables

```sql
CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(
    FALSE,
    'PRICEBOOKENTRY,TABLE1,TABLE2',  -- Comma-separated
    TRUE
);
```

### Add Business Logic Columns

```sql
INSERT INTO MS_RAW.DBT_META.STAGING_META_COLUMNS (
    COLUMN_KEY,
    SCHEMA_KEY,
    TABLE_KEY,
    COLUMN_NAME_KEY,
    SF_SCHEMA,
    SF_TABLE,
    SF_COLUMN,
    SF_DATA_TYPE,
    DBT_COLUMN_NAME,
    DBT_DATA_TYPE,
    DBT_TRANSFORMATION,
    COLUMN_SOURCE,
    COLUMN_CATEGORY,
    IS_NEW_COLUMN,
    NOTES,
    CREATED_BY
) VALUES (
    'DBO.EMPLOYEES.FULLNAME',
    'DBO',
    'DBO.EMPLOYEES',
    'FULLNAME',
    'DBO',
    'EMPLOYEES',
    'FULL_NAME',
    'VARCHAR(500)',
    'full_name',
    'VARCHAR(500)',
    'CONCAT(first_name, '' '', last_name)',
    'BUSINESS',
    'TEXT',
    TRUE,
    'Concatenated full name for convenience',
    CURRENT_USER()
);
```

### Exclude PII Columns

```sql
UPDATE MS_RAW.DBT_META.STAGING_META_COLUMNS
SET
    DBT_IS_EXCLUDED = TRUE,
    NOTES = 'PII - excluded from staging',
    UPDATED_AT = CURRENT_TIMESTAMP(),
    UPDATED_BY = CURRENT_USER()
WHERE COLUMN_KEY IN ('DBO.EMPLOYEES.SSN', 'DBO.EMPLOYEES.CREDITCARD');
```

### View Surrogate Key Plan

```sql
-- Tables needing surrogate keys
SELECT * FROM MS_RAW.DBT_META.V_COMPOSITE_PKS;

-- Complete surrogate key implementation plan
SELECT * FROM MS_RAW.DBT_META.V_SURROGATE_KEY_PLAN
ORDER BY TABLE_KEY, ORDINAL_POSITION;
```

### View All Relationships

```sql
-- All FK relationships
SELECT
    CHILD_TABLE_KEY,
    CHILD_COLUMN,
    PARENT_TABLE_KEY,
    PARENT_COLUMN,
    fk_column_count,
    CONSTRAINT_NAME
FROM MS_RAW.DBT_META.V_FK_RELATIONSHIPS
ORDER BY CHILD_TABLE_KEY;
```

### Inspect Column Transformations

```sql
SELECT
    TABLE_KEY,
    SF_COLUMN_NAME,
    DBT_COLUMN_NAME,
    SF_DATA_TYPE,
    DBT_DATA_TYPE,
    DBT_TRANSFORMATION,
    TRANSFORMATION_REASON
FROM MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS
WHERE TABLE_KEY = 'DBO.JOBS'
ORDER BY TABLE_KEY;
```

## Troubleshooting

### Pipeline Fails During Execution

1. Check the error message in the return value
2. Query execution log for details:
   ```sql
   SELECT ERROR_MESSAGE, RESULT_SUMMARY
   FROM MS_RAW.DBT_META.METADATA_BUILD_LOG
   WHERE STATUS = 'FAILED'
   ORDER BY EXECUTION_START_TIME DESC
   LIMIT 1;
   ```

### Validation Fails

Run validation separately to see detailed errors:
```sql
CALL MS_RAW.DBT_META.SP_VALIDATE_METADATA();
```

Common issues:
- **Missing PKs**: Some tables have no primary key defined in MSSQL
- **Orphaned FKs**: FK references table not included in ingestion
- **Duplicate keys**: Transformation logic created duplicate COLUMN_KEYs

### Missing Transformations

The transformation logic is defined in `SP_NORMALIZE_COLUMNS`. To customize:

1. Edit `scripts/staging_metadata/03_column_transformations.sql`
2. Redeploy: `!source scripts/staging_metadata/03_column_transformations.sql`
3. Re-run stages 3-5:
   ```sql
   CALL MS_RAW.DBT_META.SP_NORMALIZE_COLUMNS();
   CALL MS_RAW.DBT_META.SP_ANALYZE_RELATIONSHIPS();
   CALL MS_RAW.DBT_META.SP_BUILD_FINAL_METADATA();
   ```

### Performance Issues

For large databases (1000+ tables):

1. **Increase warehouse size** during execution:
   ```sql
   ALTER WAREHOUSE <warehouse_name> SET WAREHOUSE_SIZE = 'LARGE';
   CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(...);
   ALTER WAREHOUSE <warehouse_name> SET WAREHOUSE_SIZE = 'SMALL';
   ```

2. **Run stages individually** to isolate slow steps:
   ```sql
   CALL MS_RAW.DBT_META.SP_SETUP_FOUNDATION();
   CALL MS_RAW.DBT_META.SP_JOIN_METADATA('PRICEBOOKENTRY');
   -- Check duration, then continue...
   ```

## File Reference

| File | Purpose |
|------|---------|
| `00_ARCHITECTURE.md` | Detailed architecture documentation |
| `01_foundation.sql` | Creates database, schemas, UDFs, base tables |
| `02_metadata_integration.sql` | Joins SF and MS metadata |
| `03_column_transformations.sql` | Timestamp normalization, naming conventions |
| `04_relationship_analysis.sql` | PK/FK analysis, surrogate key planning |
| `05_build_final_metadata.sql` | Builds STAGING_META_COLUMNS and STAGING_META_TABLES |
| `06_validation.sql` | Data quality checks and validation |
| `07_orchestrator.sql` | Main orchestrator procedure |
| `99_deploy_all.sql` | Deploy all procedures at once |
| `README.md` | This file |

## Migration from Old Scripts

The old scripts in `sql_scripts/` are preserved for reference. Key differences:

| Old Approach | New Approach |
|--------------|--------------|
| 17+ separate SQL files | 7 stored procedures |
| Manual execution order | Single orchestrator call |
| No error handling | Comprehensive exception handling |
| No logging | Audit trail in METADATA_BUILD_LOG |
| Ad-hoc validation | Built-in validation stage |
| Repeated CTEs | Reusable views and UDFs |
| Manual state management | Idempotent procedures |

## Benefits

1. **Single Execution**: One procedure call vs 17+ scripts
2. **Error Handling**: Automatic rollback with detailed errors
3. **Idempotent**: Safe to rerun without side effects
4. **Observable**: Execution logging and validation
5. **Modular**: Each stage can run independently
6. **Performance**: Optimized with clustered tables
7. **Maintainable**: Clear separation of concerns
8. **Type Safe**: Parameter validation
9. **Version Controlled**: All logic in stored procedures

## Next Steps

After building metadata:

1. **Generate dbt Models**: Use STAGING_META_COLUMNS to generate SQL
2. **Create dbt Tests**: Use PK/FK metadata for tests
3. **Build Intermediate Models**: Use STAGING_META_TABLES for lineage
4. **Document Models**: Use DBT_DESCRIPTION field for schema.yml

## Support

For issues or questions:
1. Check execution logs: `MS_RAW.DBT_META.METADATA_BUILD_LOG`
2. Review architecture: `00_ARCHITECTURE.md`
3. Validate data: `CALL MS_RAW.DBT_META.SP_VALIDATE_METADATA()`

## Version History

- **v1.0** (2024): Initial release with 6-stage pipeline
  - Foundation setup with UDFs and base tables
  - Metadata integration (SF + MS)
  - Column transformation rules
  - PK/FK relationship analysis
  - Surrogate key generation
  - Data validation framework
