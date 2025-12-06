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
-- BONUS: Summary Statistics
-- ============================================================================
-- Quick overview of your ingested database structure

WITH ingested_tables AS (
    SELECT DISTINCT
        SCHEMA_ONLY_KEY,
        TABLE_ONLY_KEY,
        TABLE_KEY,
        MS_TABLE_SCHEMA,
        MS_TABLE_NAME
    FROM MS_RAW.DBT_META.SF_COLUMNS
    WHERE MS_TABLE_SCHEMA IS NOT NULL
        AND MS_TABLE_NAME IS NOT NULL
)
SELECT 
    'Total Ingested Tables' as METRIC,
    COUNT(DISTINCT TABLE_KEY) as COUNT
FROM ingested_tables

UNION ALL

SELECT 
    'Tables with Single-Column PK' as METRIC,
    COUNT(DISTINCT it.TABLE_KEY) as COUNT
FROM ingested_tables it
INNER JOIN (
    SELECT 
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        tc.CONSTRAINT_NAME
    FROM MS_RAW.MS_META.TABLE_CONSTRAINTS tc
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu
        ON tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        AND tc.TABLE_NAME = kcu.TABLE_NAME
        AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME
    HAVING COUNT(*) = 1
) single_pk
    ON UPPER(single_pk.TABLE_SCHEMA) = it.SCHEMA_ONLY_KEY
    AND UPPER(REPLACE(single_pk.TABLE_NAME, '_', '')) = it.TABLE_ONLY_KEY

UNION ALL

SELECT 
    'Tables with Composite PK' as METRIC,
    COUNT(DISTINCT it.TABLE_KEY) as COUNT
FROM ingested_tables it
INNER JOIN (
    SELECT 
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        tc.CONSTRAINT_NAME
    FROM MS_RAW.MS_META.TABLE_CONSTRAINTS tc
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu
        ON tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        AND tc.TABLE_NAME = kcu.TABLE_NAME
        AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME
    HAVING COUNT(*) > 1
) composite_pk
    ON UPPER(composite_pk.TABLE_SCHEMA) = it.SCHEMA_ONLY_KEY
    AND UPPER(REPLACE(composite_pk.TABLE_NAME, '_', '')) = it.TABLE_ONLY_KEY

UNION ALL

SELECT 
    'Tables with No PK' as METRIC,
    COUNT(DISTINCT it.TABLE_KEY) as COUNT
FROM ingested_tables it
LEFT JOIN MS_RAW.MS_META.TABLE_CONSTRAINTS tc
    ON UPPER(tc.TABLE_SCHEMA) = it.SCHEMA_ONLY_KEY
    AND UPPER(REPLACE(tc.TABLE_NAME, '_', '')) = it.TABLE_ONLY_KEY
    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
WHERE tc.CONSTRAINT_NAME IS NULL

UNION ALL

SELECT 
    'Total FK Relationships (Ingested Tables)' as METRIC,
    COUNT(DISTINCT rc.CONSTRAINT_NAME) as COUNT
FROM MS_RAW.MS_META.REFERENTIAL_CONSTRAINTS rc
INNER JOIN ingested_tables it_child
    ON UPPER(rc.CONSTRAINT_SCHEMA) = it_child.SCHEMA_ONLY_KEY
INNER JOIN ingested_tables it_parent
    ON UPPER(rc.UNIQUE_CONSTRAINT_SCHEMA) = it_parent.SCHEMA_ONLY_KEY;
