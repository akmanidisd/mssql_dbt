-- ============================================================================
-- PK/FK Metadata Analysis from MSSQL INFORMATION_SCHEMA in Snowflake
-- ============================================================================
-- Database: MS_RAW
-- Schema: MS_META
-- Purpose: Extract PK/FK relationships for data modeling and surrogate key planning
-- ============================================================================

USE DATABASE MS_RAW;
USE SCHEMA MS_META;

-- ============================================================================
-- 2) List of Tables Needing Surrogate Keys (SCHEMA, TABLE, proposed NAME)
-- ============================================================================
-- This query identifies ingested tables that need surrogate keys:
--   - Tables with composite PKs (2 columns)
--   - Tables with no PK
-- These are candidates for adding surrogate keys named <table>_id
-- EXCLUDES: PRICEBOOK_ENTRY table

select * from MS_RAW.DBT_META.KEY_2_PK_TWO ORDER BY ALL;
--CREATE OR REPLACE TABLE MS_RAW.DBT_META.KEY_2_PK_TWO AS
WITH ingested_tables AS (
    -- Get unique list of tables that are ingested in Snowflake
    SELECT DISTINCT
        SCHEMA_ONLY_KEY,
        TABLE_ONLY_KEY,
        TABLE_KEY,
        MS_TABLE_SCHEMA,
        MS_TABLE_NAME,
        SF_TABLE_SCHEMA,
        SF_TABLE_NAME
    FROM MS_RAW.DBT_META.SF_COLUMNS
    WHERE MS_TABLE_SCHEMA IS NOT NULL
        AND MS_TABLE_NAME IS NOT NULL
        AND UPPER(REPLACE(MS_TABLE_NAME, '_', '')) != 'PRICEBOOKENTRY'  -- Exclude PRICEBOOK_ENTRY
),
pk_columns_detail AS (
    SELECT 
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        tc.CONSTRAINT_NAME,
        kcu.COLUMN_NAME,
        kcu.ORDINAL_POSITION,
        COUNT(*) OVER (PARTITION BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME) as pk_column_count
    FROM MS_RAW.MS_META.TABLE_CONSTRAINTS tc
    INNER JOIN ingested_tables it
        ON UPPER(tc.TABLE_SCHEMA) = it.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(tc.TABLE_NAME, '_', '')) = it.TABLE_ONLY_KEY
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu
        ON tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        AND tc.TABLE_NAME = kcu.TABLE_NAME
        AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
),
pk_column_arrays AS (
    SELECT 
        TABLE_SCHEMA,
        TABLE_NAME,
        CONSTRAINT_NAME,
        pk_column_count,
        ARRAY_AGG(COLUMN_NAME) WITHIN GROUP (ORDER BY ORDINAL_POSITION) as PK_COLUMNS_ARRAY
    FROM pk_columns_detail
    GROUP BY 
        TABLE_SCHEMA,
        TABLE_NAME,
        CONSTRAINT_NAME,
        pk_column_count
),
tables_needing_surrogate AS (
    SELECT 
        it.MS_TABLE_SCHEMA,
        it.MS_TABLE_NAME,
        it.SF_TABLE_SCHEMA,
        it.SF_TABLE_NAME,
        it.SCHEMA_ONLY_KEY,
        it.TABLE_ONLY_KEY,
        COALESCE(pca.pk_column_count, 0) as current_pk_columns,
        pca.PK_COLUMNS_ARRAY,
        CASE 
            WHEN pca.pk_column_count IS NULL THEN 'NO_PK'
            WHEN pca.pk_column_count > 1 THEN 'COMPOSITE_PK'
        END as reason
    FROM ingested_tables it
    LEFT JOIN pk_column_arrays pca
        ON UPPER(pca.TABLE_SCHEMA) = it.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(pca.TABLE_NAME, '_', '')) = it.TABLE_ONLY_KEY
    WHERE pca.pk_column_count IS NULL 
        OR pca.pk_column_count > 1
)
SELECT 
    MS_TABLE_SCHEMA,
    MS_TABLE_NAME,
    SF_TABLE_SCHEMA,
    SF_TABLE_NAME,
    SF_TABLE_NAME || 'ID' as SURROGATE_KEY_NAME,
    current_pk_columns,
    PK_COLUMNS_ARRAY as COMPOSITE_PK_COLUMNS,
    reason,
    -- Standardized keys
    SCHEMA_ONLY_KEY as SF_SCHEMA_KEY,
    TABLE_ONLY_KEY as SF_TABLE_KEY,
    UPPER(REPLACE(LOWER(SF_TABLE_NAME) || '_id', '_', '')) as SF_SURROGATE_KEY
FROM tables_needing_surrogate
ORDER BY 
    reason DESC,
    MS_TABLE_SCHEMA,
    MS_TABLE_NAME;
select * from MS_RAW.DBT_META.KEY_2_PK_TWO ORDER BY ALL;
