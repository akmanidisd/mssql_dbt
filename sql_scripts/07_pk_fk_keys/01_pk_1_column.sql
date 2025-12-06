-- ============================================================================
-- PK/FK Metadata Analysis from MSSQL INFORMATION_SCHEMA in Snowflake
-- ============================================================================
-- Database: MS_RAW
-- Schema: DBT_META
-- Purpose: Extract PK/FK relationships for data modeling and surrogate key planning
-- ============================================================================

USE DATABASE MS_RAW;
USE SCHEMA MS_META;

-- ============================================================================
-- 1) List of Primary Keys with 1 Column (SCHEMA, TABLE, COLUMN)
-- ============================================================================
-- This query identifies all single-column primary keys
-- Useful for understanding which tables already have simple PKs
select * from MS_RAW.DBT_META.KEY_1_PK_ONE ORDER BY ALL;
CREATE OR REPLACE TABLE MS_RAW.DBT_META.KEY_1_PK_ONE AS
WITH pk_constraints AS (
    SELECT 
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        tc.CONSTRAINT_NAME,
        COUNT(*) as column_count
    FROM TABLE_CONSTRAINTS tc
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    GROUP BY 
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        tc.CONSTRAINT_NAME
),
single_column_pks AS (
    SELECT 
        kcu.TABLE_SCHEMA,
        kcu.TABLE_NAME,
        kcu.COLUMN_NAME,
        kcu.CONSTRAINT_NAME,
        kcu.ORDINAL_POSITION
    FROM KEY_COLUMN_USAGE kcu
    INNER JOIN pk_constraints pc
        ON kcu.TABLE_SCHEMA = pc.TABLE_SCHEMA
        AND kcu.TABLE_NAME = pc.TABLE_NAME
        AND kcu.CONSTRAINT_NAME = pc.CONSTRAINT_NAME
    WHERE pc.column_count = 1
)
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    -- Standardized names for Snowflake matching
    UPPER(REPLACE((TABLE_SCHEMA || '.' || TABLE_NAME || '.' || COLUMN_NAME),'_',''))
            AS COLUMN_KEY
FROM single_column_pks
WHERE COLUMN_KEY IN (SELECT COLUMN_KEY FROM MS_RAW.DBT_META.SF_COLUMNS) 
ORDER BY 
    COLUMN_KEY;
select * from MS_RAW.DBT_META.KEY_1_PK_ONE ORDER BY ALL;
