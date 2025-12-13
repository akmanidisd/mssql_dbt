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
-- 3) List of Tables Using Foreign Keys - Candidates for Surrogate Key Addition
-- ============================================================================
-- This query identifies ingested tables that have FKs pointing to tables that need surrogate keys
-- Shows which child tables will need their FK columns updated to reference the new surrogate key
-- EXCLUDES: PRICEBOOK_ENTRY table

SELECT * FROM MS_RAW.DBT_META.KEY_3_FK_TWO ORDER BY ALL;

CREATE OR REPLACE TABLE MS_RAW.DBT_META.KEY_3_FK_TWO AS
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
composite_or_no_pk_tables AS (
    -- Identify parent tables that need surrogate keys (composite PK - 2 columns)
    SELECT DISTINCT
        TABLE_SCHEMA,
        TABLE_NAME,
        pk_column_count
    FROM pk_columns_detail
    WHERE pk_column_count > 1
    
    UNION
    
    -- Tables with no PK at all (from ingested tables)
    SELECT 
        it.MS_TABLE_SCHEMA as TABLE_SCHEMA,
        it.MS_TABLE_NAME as TABLE_NAME,
        0 as pk_column_count
    FROM ingested_tables it
    LEFT JOIN MS_RAW.MS_META.TABLE_CONSTRAINTS tc
        ON UPPER(it.MS_TABLE_SCHEMA) = it.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(it.MS_TABLE_NAME, '_', '')) = it.TABLE_ONLY_KEY
        AND UPPER(tc.TABLE_SCHEMA) = it.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(tc.TABLE_NAME, '_', '')) = it.TABLE_ONLY_KEY
        AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    WHERE tc.CONSTRAINT_NAME IS NULL
),
fk_relationships AS (
    SELECT 
        rc.CONSTRAINT_SCHEMA as FK_SCHEMA,
        kcu_fk.TABLE_NAME as FK_TABLE,
        kcu_fk.COLUMN_NAME as FK_COLUMN,
        kcu_fk.ORDINAL_POSITION as FK_ORDINAL,
        rc.UNIQUE_CONSTRAINT_SCHEMA as PK_SCHEMA,
        kcu_pk.TABLE_NAME as PK_TABLE,
        kcu_pk.COLUMN_NAME as PK_COLUMN,
        rc.CONSTRAINT_NAME as FK_CONSTRAINT_NAME
    FROM MS_RAW.MS_META.REFERENTIAL_CONSTRAINTS rc
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_fk
        ON rc.CONSTRAINT_SCHEMA = kcu_fk.CONSTRAINT_SCHEMA
        AND rc.CONSTRAINT_NAME = kcu_fk.CONSTRAINT_NAME
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_pk
        ON rc.UNIQUE_CONSTRAINT_SCHEMA = kcu_pk.CONSTRAINT_SCHEMA
        AND rc.UNIQUE_CONSTRAINT_NAME = kcu_pk.CONSTRAINT_NAME
        AND kcu_fk.ORDINAL_POSITION = kcu_pk.ORDINAL_POSITION
),
fk_to_surrogate_candidates AS (
    SELECT 
        fk.FK_SCHEMA,
        fk.FK_TABLE,
        fk.FK_CONSTRAINT_NAME,
        ARRAY_AGG(fk.FK_COLUMN) WITHIN GROUP (ORDER BY fk.FK_ORDINAL) as FK_COLUMNS_ARRAY,
        COUNT(*) as FK_COLUMN_COUNT,
        fk.PK_SCHEMA,
        fk.PK_TABLE,
        ARRAY_AGG(fk.PK_COLUMN) WITHIN GROUP (ORDER BY fk.FK_ORDINAL) as PK_COLUMNS_ARRAY,
        cp.pk_column_count as PARENT_PK_COLUMNS
    FROM fk_relationships fk
    INNER JOIN composite_or_no_pk_tables cp
        ON fk.PK_SCHEMA = cp.TABLE_SCHEMA
        AND fk.PK_TABLE = cp.TABLE_NAME
    -- Filter to only FKs where the child table is also ingested
    INNER JOIN ingested_tables it_child
        ON UPPER(fk.FK_SCHEMA) = it_child.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(fk.FK_TABLE, '_', '')) = it_child.TABLE_ONLY_KEY
    -- Filter to only FKs where the parent table is also ingested
    INNER JOIN ingested_tables it_parent
        ON UPPER(fk.PK_SCHEMA) = it_parent.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(fk.PK_TABLE, '_', '')) = it_parent.TABLE_ONLY_KEY
    GROUP BY 
        fk.FK_SCHEMA,
        fk.FK_TABLE,
        fk.FK_CONSTRAINT_NAME,
        fk.PK_SCHEMA,
        fk.PK_TABLE,
        cp.pk_column_count
)
SELECT 
    fk.FK_SCHEMA as MS_CHILD_SCHEMA,
    fk.FK_TABLE as MS_CHILD_TABLE,
    it_child.SF_TABLE_SCHEMA as SF_CHILD_SCHEMA,
    it_child.SF_TABLE_NAME as SF_CHILD_TABLE,
    fk.FK_COLUMNS_ARRAY as CURRENT_FK_COLUMNS,
    fk.FK_COLUMN_COUNT,
    fk.PK_SCHEMA as MS_PARENT_SCHEMA,
    fk.PK_TABLE as MS_PARENT_TABLE,
    it_parent.SF_TABLE_SCHEMA as SF_PARENT_SCHEMA,
    it_parent.SF_TABLE_NAME as SF_PARENT_TABLE,
    fk.PK_COLUMNS_ARRAY as PARENT_PK_COLUMNS,
    LOWER(it_parent.SF_TABLE_NAME) || '_id' as SURROGATE_KEY_TO_ADD,
    fk.FK_CONSTRAINT_NAME,
    CASE 
        WHEN fk.PARENT_PK_COLUMNS = 0 THEN 'PARENT_NO_PK'
        ELSE 'PARENT_COMPOSITE_PK'
    END as PARENT_STATUS,
    -- Standardized keys for Snowflake
    it_child.SCHEMA_ONLY_KEY as SF_CHILD_SCHEMA_KEY,
    it_child.TABLE_ONLY_KEY as SF_CHILD_TABLE_KEY,
    it_parent.SCHEMA_ONLY_KEY as SF_PARENT_SCHEMA_KEY,
    it_parent.TABLE_ONLY_KEY as SF_PARENT_TABLE_KEY,
    UPPER(REPLACE(LOWER(it_parent.SF_TABLE_NAME) || '_id', '_', '')) as SF_SURROGATE_KEY
FROM fk_to_surrogate_candidates fk
INNER JOIN ingested_tables it_child
    ON UPPER(fk.FK_SCHEMA) = it_child.SCHEMA_ONLY_KEY
    AND UPPER(REPLACE(fk.FK_TABLE, '_', '')) = it_child.TABLE_ONLY_KEY
INNER JOIN ingested_tables it_parent
    ON UPPER(fk.PK_SCHEMA) = it_parent.SCHEMA_ONLY_KEY
    AND UPPER(REPLACE(fk.PK_TABLE, '_', '')) = it_parent.TABLE_ONLY_KEY
ORDER BY 
    MS_PARENT_SCHEMA,
    MS_PARENT_TABLE,
    MS_CHILD_SCHEMA,
    MS_CHILD_TABLE;

select * from MS_RAW.DBT_META.KEY_3_FK_TWO ORDER BY ALL;
