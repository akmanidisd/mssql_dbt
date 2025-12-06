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
-- META_COLUMNS Table Structure Proposal
-- ============================================================================
-- Purpose: Unified metadata table for managing column transformations from
--          MSSQL -> Snowflake RAW -> dbt STAGING
-- Usage: Generate dbt staging models with proper typing, PKs, and FKs
-- ============================================================================

-- ============================================================================
-- TABLE DEFINITION
-- ============================================================================

CREATE OR REPLACE TABLE MS_RAW.DBT_META.META_COLUMNS (
    
    -- ========================================================================
    -- SECTION 1: IDENTITY & KEYS
    -- ========================================================================
    -- Standardized keys for joining and uniqueness
    SCHEMA_ONLY_KEY         VARCHAR(16777216)   NOT NULL,
    TABLE_ONLY_KEY          VARCHAR(16777216)   NOT NULL,
    COLUMN_ONLY_KEY         VARCHAR(16777216)   NOT NULL,
    TABLE_KEY               VARCHAR(16777216)   NOT NULL,  -- schema.table
    COLUMN_KEY              VARCHAR(16777216)   NOT NULL,  -- schema.table.column (PRIMARY KEY)
    
    -- ========================================================================
    -- SECTION 2: MSSQL SOURCE METADATA
    -- ========================================================================
    MS_TABLE_SCHEMA         VARCHAR(16777216),
    MS_TABLE_NAME           VARCHAR(16777216),
    MS_COLUMN_NAME          VARCHAR(16777216),
    MS_ORDINAL_POSITION     NUMBER(19,0),
    MS_DATA_TYPE            VARCHAR(16777216),
    MS_IS_NULLABLE          VARCHAR(16777216),
    MS_CHARACTER_MAXIMUM_LENGTH NUMBER(19,0),
    MS_NUMERIC_PRECISION    NUMBER(19,0),
    MS_NUMERIC_SCALE        NUMBER(19,0),
    MS_COLUMN_DEFAULT       VARCHAR(16777216),
    
    -- ========================================================================
    -- SECTION 3: SNOWFLAKE RAW METADATA
    -- ========================================================================
    SF_RAW_TABLE_SCHEMA     VARCHAR(16777216),
    SF_RAW_TABLE_NAME       VARCHAR(16777216),
    SF_RAW_COLUMN_NAME      VARCHAR(16777216),
    SF_RAW_ORDINAL_POSITION NUMBER(38,0),
    SF_RAW_DATA_TYPE        VARCHAR(16777216),
    SF_RAW_IS_NULLABLE      VARCHAR(3),
    SF_RAW_CHARACTER_MAXIMUM_LENGTH NUMBER(38,0),
    SF_RAW_NUMERIC_PRECISION NUMBER(38,0),
    SF_RAW_NUMERIC_SCALE    NUMBER(38,0),
    
    -- ========================================================================
    -- SECTION 4: DBT STAGING DEFINITION
    -- ========================================================================
    -- The target state for staging models
    DBT_STG_COLUMN_NAME     VARCHAR(16777216),  -- If NULL, column is ignored/excluded
    DBT_STG_DATA_TYPE       VARCHAR(16777216),  -- Target data type (can differ from RAW)
    DBT_STG_IS_NULLABLE     VARCHAR(3),         -- 'YES', 'NO', or NULL
    DBT_STG_DESCRIPTION     VARCHAR(16777216),  -- Column description for documentation
    DBT_STG_TRANSFORMATION  VARCHAR(16777216),  -- SQL logic if transform needed (e.g., 'UPPER(column_name)')
    DBT_STG_ORDINAL_POSITION NUMBER(38,0),      -- Display order in staging model
    DBT_STG_IS_EXCLUDED     BOOLEAN DEFAULT FALSE,  -- TRUE = exclude from staging
    
    -- ========================================================================
    -- SECTION 5: PRIMARY KEY METADATA
    -- ========================================================================
    -- Simplified: one column per PK (use surrogate keys for composite)
    IS_PRIMARY_KEY          BOOLEAN DEFAULT FALSE,
    PK_CONSTRAINT_NAME      VARCHAR(16777216),  -- Source PK constraint name
    PK_ORDINAL_POSITION     NUMBER(38,0),       -- Position if part of composite (for reference)
    IS_SURROGATE_KEY        BOOLEAN DEFAULT FALSE,  -- TRUE if this is a generated surrogate key
    
    -- ========================================================================
    -- SECTION 6: FOREIGN KEY METADATA
    -- ========================================================================
    IS_FOREIGN_KEY          BOOLEAN DEFAULT FALSE,
    FK_CONSTRAINT_NAME      VARCHAR(16777216),  -- Source FK constraint name
    FK_PARENT_SCHEMA_KEY    VARCHAR(16777216),  -- Parent table's SCHEMA_ONLY_KEY
    FK_PARENT_TABLE_KEY     VARCHAR(16777216),  -- Parent table's TABLE_ONLY_KEY
    FK_PARENT_COLUMN_KEY    VARCHAR(16777216),  -- Parent column's COLUMN_ONLY_KEY
    FK_PARENT_FULL_KEY      VARCHAR(16777216),  -- Full parent: schema.table.column
    FK_ORDINAL_POSITION     NUMBER(38,0),       -- Position if multi-column FK
    
    -- ========================================================================
    -- SECTION 7: SOURCE TRACKING & METADATA
    -- ========================================================================
    COLUMN_SOURCE           VARCHAR(20),        -- 'MSSQL', 'DERIVED', 'SURROGATE', 'BUSINESS'
    IS_NEW_COLUMN           BOOLEAN DEFAULT FALSE,  -- TRUE if doesn't exist in RAW
    NOTES                   VARCHAR(16777216),  -- Free-form notes
    CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY              VARCHAR(255),
    UPDATED_BY              VARCHAR(255),
    
    -- ========================================================================
    -- CONSTRAINTS
    -- ========================================================================
    CONSTRAINT PK_META_COLUMNS PRIMARY KEY (COLUMN_KEY)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================
-- Consider adding these if the table grows large:
-- CREATE INDEX IDX_META_COLUMNS_TABLE_KEY ON META_COLUMNS(TABLE_KEY);
-- CREATE INDEX IDX_META_COLUMNS_FK_PARENT ON META_COLUMNS(FK_PARENT_FULL_KEY);


-- ============================================================================
-- INITIAL POPULATION QUERY
-- ============================================================================
-- Step 1: Load existing MS + SF RAW columns (from SF_COLUMNS)

INSERT INTO MS_RAW.DBT_META.META_COLUMNS (
    SCHEMA_ONLY_KEY,
    TABLE_ONLY_KEY,
    COLUMN_ONLY_KEY,
    TABLE_KEY,
    COLUMN_KEY,
    -- MS metadata
    MS_TABLE_SCHEMA,
    MS_TABLE_NAME,
    MS_COLUMN_NAME,
    MS_ORDINAL_POSITION,
    MS_DATA_TYPE,
    MS_IS_NULLABLE,
    MS_CHARACTER_MAXIMUM_LENGTH,
    MS_NUMERIC_PRECISION,
    MS_NUMERIC_SCALE,
    MS_COLUMN_DEFAULT,
    -- SF RAW metadata
    SF_RAW_TABLE_SCHEMA,
    SF_RAW_TABLE_NAME,
    SF_RAW_COLUMN_NAME,
    SF_RAW_ORDINAL_POSITION,
    SF_RAW_DATA_TYPE,
    SF_RAW_IS_NULLABLE,
    SF_RAW_CHARACTER_MAXIMUM_LENGTH,
    SF_RAW_NUMERIC_PRECISION,
    SF_RAW_NUMERIC_SCALE,
    -- DBT Staging defaults (to be updated)
    DBT_STG_COLUMN_NAME,
    DBT_STG_DATA_TYPE,
    DBT_STG_IS_NULLABLE,
    DBT_STG_ORDINAL_POSITION,
    DBT_STG_IS_EXCLUDED,
    -- Source tracking
    COLUMN_SOURCE,
    IS_NEW_COLUMN,
    CREATED_BY
)
SELECT 
    -- Keys
    sfc.SCHEMA_ONLY_KEY,
    sfc.TABLE_ONLY_KEY,
    sfc.COLUMN_ONLY_KEY,
    sfc.TABLE_KEY,
    sfc.COLUMN_KEY,
    
    -- MS metadata
    sfc.MS_TABLE_SCHEMA,
    sfc.MS_TABLE_NAME,
    sfc.MS_COLUMN_NAME,
    sfc.MS_ORDINAL_POSITION,
    sfc.MS_DATA_TYPE,
    sfc.MS_IS_NULLABLE,
    sfc.MS_CHARACTER_MAXIMUM_LENGTH,
    sfc.MS_NUMERIC_PRECISION,
    sfc.MS_NUMERIC_SCALE,
    sfc.MS_COLUMN_DEFAULT,
    
    -- SF RAW metadata
    sfc.SF_TABLE_SCHEMA,
    sfc.SF_TABLE_NAME,
    sfc.SF_COLUMN_NAME,
    sfc.SF_ORDINAL_POSITION,
    sfc.SF_DATA_TYPE,
    sfc.SF_IS_NULLABLE,
    sfc.SF_CHARACTER_MAXIMUM_LENGTH,
    sfc.SF_NUMERIC_PRECISION,
    sfc.SF_NUMERIC_SCALE,
    
    -- DBT Staging defaults (initially same as RAW)
    sfc.SF_COLUMN_NAME as DBT_STG_COLUMN_NAME,
    sfc.SF_DATA_TYPE as DBT_STG_DATA_TYPE,
    sfc.SF_IS_NULLABLE as DBT_STG_IS_NULLABLE,
    sfc.SF_ORDINAL_POSITION as DBT_STG_ORDINAL_POSITION,
    FALSE as DBT_STG_IS_EXCLUDED,
    
    -- Source tracking
    'MSSQL' as COLUMN_SOURCE,
    FALSE as IS_NEW_COLUMN,
    CURRENT_USER() as CREATED_BY
    
FROM MS_RAW.DBT_META.SF_COLUMNS sfc
WHERE sfc.MS_TABLE_SCHEMA IS NOT NULL
  AND sfc.MS_TABLE_NAME IS NOT NULL
  AND UPPER(REPLACE(sfc.MS_TABLE_NAME, '_', '')) != 'PRICEBOOKENTRY'  -- Exclude as needed
ORDER BY sfc.COLUMN_KEY;


-- ============================================================================
-- Step 2: Update PRIMARY KEY information
-- ============================================================================

MERGE INTO MS_RAW.DBT_META.META_COLUMNS mc
USING (
    WITH ingested_tables AS (
        SELECT DISTINCT
            SCHEMA_ONLY_KEY,
            TABLE_ONLY_KEY,
            MS_TABLE_SCHEMA,
            MS_TABLE_NAME
        FROM MS_RAW.DBT_META.SF_COLUMNS
        WHERE MS_TABLE_SCHEMA IS NOT NULL
          AND MS_TABLE_NAME IS NOT NULL
          AND UPPER(REPLACE(MS_TABLE_NAME, '_', '')) != 'PRICEBOOKENTRY'
    ),
    pk_info AS (
        SELECT 
            UPPER(kcu.TABLE_SCHEMA) as SCHEMA_KEY,
            UPPER(REPLACE(kcu.TABLE_NAME, '_', '')) as TABLE_KEY,
            UPPER(REPLACE(kcu.COLUMN_NAME, '_', '')) as COLUMN_KEY,
            kcu.TABLE_SCHEMA,
            kcu.TABLE_NAME,
            kcu.COLUMN_NAME,
            kcu.CONSTRAINT_NAME,
            kcu.ORDINAL_POSITION,
            COUNT(*) OVER (PARTITION BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME) as pk_column_count
        FROM MS_RAW.MS_META.KEY_COLUMN_USAGE kcu
        INNER JOIN MS_RAW.MS_META.TABLE_CONSTRAINTS tc
            ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
            AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
        INNER JOIN ingested_tables it
            ON UPPER(kcu.TABLE_SCHEMA) = it.SCHEMA_ONLY_KEY
            AND UPPER(REPLACE(kcu.TABLE_NAME, '_', '')) = it.TABLE_ONLY_KEY
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    )
    SELECT 
        it.SCHEMA_ONLY_KEY || '.' || it.TABLE_ONLY_KEY || '.' || pk.COLUMN_KEY as COLUMN_KEY,
        TRUE as IS_PRIMARY_KEY,
        pk.CONSTRAINT_NAME as PK_CONSTRAINT_NAME,
        pk.ORDINAL_POSITION as PK_ORDINAL_POSITION,
        FALSE as IS_SURROGATE_KEY
    FROM pk_info pk
    INNER JOIN ingested_tables it
        ON pk.SCHEMA_KEY = it.SCHEMA_ONLY_KEY
        AND pk.TABLE_KEY = it.TABLE_ONLY_KEY
    WHERE pk.pk_column_count = 1  -- Only single-column PKs
) pk_data
ON mc.COLUMN_KEY = pk_data.COLUMN_KEY
WHEN MATCHED THEN UPDATE SET
    mc.IS_PRIMARY_KEY = pk_data.IS_PRIMARY_KEY,
    mc.PK_CONSTRAINT_NAME = pk_data.PK_CONSTRAINT_NAME,
    mc.PK_ORDINAL_POSITION = pk_data.PK_ORDINAL_POSITION,
    mc.IS_SURROGATE_KEY = pk_data.IS_SURROGATE_KEY,
    mc.UPDATED_AT = CURRENT_TIMESTAMP(),
    mc.UPDATED_BY = CURRENT_USER();


-- ============================================================================
-- Step 3: Update FOREIGN KEY information
-- ============================================================================

MERGE INTO MS_RAW.DBT_META.META_COLUMNS mc
USING (
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
          AND UPPER(REPLACE(MS_TABLE_NAME, '_', '')) != 'PRICEBOOKENTRY'
    ),
    fk_info AS (
        SELECT 
            rc.CONSTRAINT_NAME,
            kcu_fk.TABLE_SCHEMA as FK_SCHEMA,
            kcu_fk.TABLE_NAME as FK_TABLE,
            kcu_fk.COLUMN_NAME as FK_COLUMN,
            kcu_fk.ORDINAL_POSITION as FK_ORDINAL,
            kcu_pk.TABLE_SCHEMA as PK_SCHEMA,
            kcu_pk.TABLE_NAME as PK_TABLE,
            kcu_pk.COLUMN_NAME as PK_COLUMN,
            COUNT(*) OVER (PARTITION BY rc.CONSTRAINT_NAME) as fk_column_count
        FROM MS_RAW.MS_META.REFERENTIAL_CONSTRAINTS rc
        INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_fk
            ON rc.CONSTRAINT_SCHEMA = kcu_fk.CONSTRAINT_SCHEMA
            AND rc.CONSTRAINT_NAME = kcu_fk.CONSTRAINT_NAME
        INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_pk
            ON rc.UNIQUE_CONSTRAINT_SCHEMA = kcu_pk.CONSTRAINT_SCHEMA
            AND rc.UNIQUE_CONSTRAINT_NAME = kcu_pk.CONSTRAINT_NAME
            AND kcu_fk.ORDINAL_POSITION = kcu_pk.ORDINAL_POSITION
        INNER JOIN ingested_tables it_child
            ON UPPER(kcu_fk.TABLE_SCHEMA) = it_child.SCHEMA_ONLY_KEY
            AND UPPER(REPLACE(kcu_fk.TABLE_NAME, '_', '')) = it_child.TABLE_ONLY_KEY
        INNER JOIN ingested_tables it_parent
            ON UPPER(kcu_pk.TABLE_SCHEMA) = it_parent.SCHEMA_ONLY_KEY
            AND UPPER(REPLACE(kcu_pk.TABLE_NAME, '_', '')) = it_parent.TABLE_ONLY_KEY
    )
    SELECT 
        UPPER(fk.FK_SCHEMA) || '.' || UPPER(REPLACE(fk.FK_TABLE, '_', '')) || '.' || UPPER(REPLACE(fk.FK_COLUMN, '_', '')) as FK_COLUMN_KEY,
        TRUE as IS_FOREIGN_KEY,
        fk.CONSTRAINT_NAME as FK_CONSTRAINT_NAME,
        UPPER(fk.PK_SCHEMA) as FK_PARENT_SCHEMA_KEY,
        UPPER(REPLACE(fk.PK_TABLE, '_', '')) as FK_PARENT_TABLE_KEY,
        UPPER(REPLACE(fk.PK_COLUMN, '_', '')) as FK_PARENT_COLUMN_KEY,
        UPPER(fk.PK_SCHEMA) || '.' || UPPER(REPLACE(fk.PK_TABLE, '_', '')) || '.' || UPPER(REPLACE(fk.PK_COLUMN, '_', '')) as FK_PARENT_FULL_KEY,
        fk.FK_ORDINAL as FK_ORDINAL_POSITION
    FROM fk_info fk
    WHERE fk.fk_column_count = 1  -- Only single-column FKs for now
) fk_data
ON mc.COLUMN_KEY = fk_data.FK_COLUMN_KEY
WHEN MATCHED THEN UPDATE SET
    mc.IS_FOREIGN_KEY = fk_data.IS_FOREIGN_KEY,
    mc.FK_CONSTRAINT_NAME = fk_data.FK_CONSTRAINT_NAME,
    mc.FK_PARENT_SCHEMA_KEY = fk_data.FK_PARENT_SCHEMA_KEY,
    mc.FK_PARENT_TABLE_KEY = fk_data.FK_PARENT_TABLE_KEY,
    mc.FK_PARENT_COLUMN_KEY = fk_data.FK_PARENT_COLUMN_KEY,
    mc.FK_PARENT_FULL_KEY = fk_data.FK_PARENT_FULL_KEY,
    mc.FK_ORDINAL_POSITION = fk_data.FK_ORDINAL_POSITION,
    mc.UPDATED_AT = CURRENT_TIMESTAMP(),
    mc.UPDATED_BY = CURRENT_USER();


-- ============================================================================
-- Step 4: Add SURROGATE KEY columns for tables with composite PKs
-- ============================================================================

INSERT INTO MS_RAW.DBT_META.META_COLUMNS (
    SCHEMA_ONLY_KEY,
    TABLE_ONLY_KEY,
    COLUMN_ONLY_KEY,
    TABLE_KEY,
    COLUMN_KEY,
    SF_RAW_TABLE_SCHEMA,
    SF_RAW_TABLE_NAME,
    SF_RAW_COLUMN_NAME,
    DBT_STG_COLUMN_NAME,
    DBT_STG_DATA_TYPE,
    DBT_STG_IS_NULLABLE,
    DBT_STG_ORDINAL_POSITION,
    DBT_STG_TRANSFORMATION,
    IS_PRIMARY_KEY,
    IS_SURROGATE_KEY,
    IS_FOREIGN_KEY,
    FK_PARENT_SCHEMA_KEY,
    FK_PARENT_TABLE_KEY,
    FK_PARENT_COLUMN_KEY,
    FK_PARENT_FULL_KEY,
    COLUMN_SOURCE,
    IS_NEW_COLUMN,
    NOTES,
    CREATED_BY
)
WITH composite_pk_tables AS (
    SELECT 
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        COUNT(*) as pk_column_count
    FROM MS_RAW.MS_META.TABLE_CONSTRAINTS tc
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu
        ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
        AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME
    HAVING COUNT(*) > 1
),
ingested_composite_tables AS (
    SELECT DISTINCT
        sfc.SCHEMA_ONLY_KEY,
        sfc.TABLE_ONLY_KEY,
        sfc.TABLE_KEY,
        sfc.SF_TABLE_SCHEMA,
        sfc.SF_TABLE_NAME
    FROM MS_RAW.DBT_META.SF_COLUMNS sfc
    INNER JOIN composite_pk_tables cpt
        ON UPPER(sfc.MS_TABLE_SCHEMA) = sfc.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(sfc.MS_TABLE_NAME, '_', '')) = sfc.TABLE_ONLY_KEY
        AND UPPER(cpt.TABLE_SCHEMA) = sfc.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(cpt.TABLE_NAME, '_', '')) = sfc.TABLE_ONLY_KEY
    WHERE sfc.MS_TABLE_SCHEMA IS NOT NULL
      AND sfc.MS_TABLE_NAME IS NOT NULL
      AND UPPER(REPLACE(sfc.MS_TABLE_NAME, '_', '')) != 'PRICEBOOKENTRY'
),
-- Part A: Surrogate keys as PRIMARY KEYS in parent tables
parent_surrogate_keys AS (
    SELECT DISTINCT
        SCHEMA_ONLY_KEY,
        TABLE_ONLY_KEY,
        UPPER(REPLACE(LOWER(SF_TABLE_NAME) || '_id', '_', '')) as COLUMN_ONLY_KEY,
        TABLE_KEY,
        TABLE_KEY || '.' || UPPER(REPLACE(LOWER(SF_TABLE_NAME) || '_id', '_', '')) as COLUMN_KEY,
        SF_TABLE_SCHEMA,
        SF_TABLE_NAME,
        NULL as SF_RAW_COLUMN_NAME,
        LOWER(SF_TABLE_NAME) || '_id' as DBT_STG_COLUMN_NAME,
        'NUMBER(38,0)' as DBT_STG_DATA_TYPE,
        'NO' as DBT_STG_IS_NULLABLE,
        0 as DBT_STG_ORDINAL_POSITION,
        '{{ dbt_utils.generate_surrogate_key([...]) }}' as DBT_STG_TRANSFORMATION,
        TRUE as IS_PRIMARY_KEY,
        TRUE as IS_SURROGATE_KEY,
        FALSE as IS_FOREIGN_KEY,
        NULL as FK_PARENT_SCHEMA_KEY,
        NULL as FK_PARENT_TABLE_KEY,
        NULL as FK_PARENT_COLUMN_KEY,
        NULL as FK_PARENT_FULL_KEY,
        'SURROGATE' as COLUMN_SOURCE,
        TRUE as IS_NEW_COLUMN,
        'Generated surrogate key for composite PK table' as NOTES,
        CURRENT_USER() as CREATED_BY
    FROM ingested_composite_tables
),
-- Part B: Find child tables that have 2-column FKs to these composite PK tables
ingested_tables AS (
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
      AND UPPER(REPLACE(MS_TABLE_NAME, '_', '')) != 'PRICEBOOKENTRY'
),
fk_relationships AS (
    SELECT 
        rc.CONSTRAINT_NAME,
        kcu_fk.TABLE_SCHEMA as FK_SCHEMA,
        kcu_fk.TABLE_NAME as FK_TABLE,
        kcu_fk.COLUMN_NAME as FK_COLUMN,
        kcu_fk.ORDINAL_POSITION as FK_ORDINAL,
        kcu_pk.TABLE_SCHEMA as PK_SCHEMA,
        kcu_pk.TABLE_NAME as PK_TABLE,
        kcu_pk.COLUMN_NAME as PK_COLUMN,
        COUNT(*) OVER (PARTITION BY rc.CONSTRAINT_NAME) as fk_column_count
    FROM MS_RAW.MS_META.REFERENTIAL_CONSTRAINTS rc
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_fk
        ON rc.CONSTRAINT_SCHEMA = kcu_fk.CONSTRAINT_SCHEMA
        AND rc.CONSTRAINT_NAME = kcu_fk.CONSTRAINT_NAME
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_pk
        ON rc.UNIQUE_CONSTRAINT_SCHEMA = kcu_pk.CONSTRAINT_SCHEMA
        AND rc.UNIQUE_CONSTRAINT_NAME = kcu_pk.CONSTRAINT_NAME
        AND kcu_fk.ORDINAL_POSITION = kcu_pk.ORDINAL_POSITION
    INNER JOIN ingested_tables it_child
        ON UPPER(kcu_fk.TABLE_SCHEMA) = it_child.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(kcu_fk.TABLE_NAME, '_', '')) = it_child.TABLE_ONLY_KEY
    INNER JOIN ingested_tables it_parent
        ON UPPER(kcu_pk.TABLE_SCHEMA) = it_parent.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(kcu_pk.TABLE_NAME, '_', '')) = it_parent.TABLE_ONLY_KEY
),
child_surrogate_fks AS (
    SELECT DISTINCT
        it_child.SCHEMA_ONLY_KEY,
        it_child.TABLE_ONLY_KEY,
        UPPER(REPLACE(LOWER(it_parent.SF_TABLE_NAME) || '_id', '_', '')) as COLUMN_ONLY_KEY,
        it_child.TABLE_KEY,
        it_child.TABLE_KEY || '.' || UPPER(REPLACE(LOWER(it_parent.SF_TABLE_NAME) || '_id', '_', '')) as COLUMN_KEY,
        it_child.SF_TABLE_SCHEMA,
        it_child.SF_TABLE_NAME,
        NULL as SF_RAW_COLUMN_NAME,
        LOWER(it_parent.SF_TABLE_NAME) || '_id' as DBT_STG_COLUMN_NAME,
        'NUMBER(38,0)' as DBT_STG_DATA_TYPE,
        'YES' as DBT_STG_IS_NULLABLE,
        999 as DBT_STG_ORDINAL_POSITION,
        NULL as DBT_STG_TRANSFORMATION,
        FALSE as IS_PRIMARY_KEY,
        FALSE as IS_SURROGATE_KEY,
        TRUE as IS_FOREIGN_KEY,
        it_parent.SCHEMA_ONLY_KEY as FK_PARENT_SCHEMA_KEY,
        it_parent.TABLE_ONLY_KEY as FK_PARENT_TABLE_KEY,
        UPPER(REPLACE(LOWER(it_parent.SF_TABLE_NAME) || '_id', '_', '')) as FK_PARENT_COLUMN_KEY,
        it_parent.TABLE_KEY || '.' || UPPER(REPLACE(LOWER(it_parent.SF_TABLE_NAME) || '_id', '_', '')) as FK_PARENT_FULL_KEY,
        'SURROGATE' as COLUMN_SOURCE,
        TRUE as IS_NEW_COLUMN,
        'Surrogate FK replacing 2-column FK: ' || fk.CONSTRAINT_NAME as NOTES,
        CURRENT_USER() as CREATED_BY
    FROM fk_relationships fk
    INNER JOIN composite_pk_tables cpt
        ON fk.PK_SCHEMA = cpt.TABLE_SCHEMA
        AND fk.PK_TABLE = cpt.TABLE_NAME
    INNER JOIN ingested_tables it_child
        ON UPPER(fk.FK_SCHEMA) = it_child.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(fk.FK_TABLE, '_', '')) = it_child.TABLE_ONLY_KEY
    INNER JOIN ingested_tables it_parent
        ON UPPER(fk.PK_SCHEMA) = it_parent.SCHEMA_ONLY_KEY
        AND UPPER(REPLACE(fk.PK_TABLE, '_', '')) = it_parent.TABLE_ONLY_KEY
    WHERE fk.fk_column_count = 2  -- Only 2-column composite FKs
)
-- Combine both parent surrogate PKs and child surrogate FKs
SELECT * FROM parent_surrogate_keys
UNION ALL
SELECT * FROM child_surrogate_fks;


-- ============================================================================
-- Step 5: Clear FK metadata from original 2-column FK columns
-- ============================================================================
-- These columns will remain in staging but are no longer marked as FKs
-- The surrogate key column becomes the new FK to the parent

MERGE INTO MS_RAW.DBT_META.META_COLUMNS mc
USING (
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
          AND UPPER(REPLACE(MS_TABLE_NAME, '_', '')) != 'PRICEBOOKENTRY'
    ),
    composite_pk_tables AS (
        SELECT 
            tc.TABLE_SCHEMA,
            tc.TABLE_NAME,
            COUNT(*) as pk_column_count
        FROM MS_RAW.MS_META.TABLE_CONSTRAINTS tc
        INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        GROUP BY tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME
        HAVING COUNT(*) > 1
    ),
    fk_relationships AS (
        SELECT 
            rc.CONSTRAINT_NAME,
            kcu_fk.TABLE_SCHEMA as FK_SCHEMA,
            kcu_fk.TABLE_NAME as FK_TABLE,
            kcu_fk.COLUMN_NAME as FK_COLUMN,
            kcu_fk.ORDINAL_POSITION as FK_ORDINAL,
            kcu_pk.TABLE_SCHEMA as PK_SCHEMA,
            kcu_pk.TABLE_NAME as PK_TABLE,
            COUNT(*) OVER (PARTITION BY rc.CONSTRAINT_NAME) as fk_column_count
        FROM MS_RAW.MS_META.REFERENTIAL_CONSTRAINTS rc
        INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_fk
            ON rc.CONSTRAINT_SCHEMA = kcu_fk.CONSTRAINT_SCHEMA
            AND rc.CONSTRAINT_NAME = kcu_fk.CONSTRAINT_NAME
        INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_pk
            ON rc.UNIQUE_CONSTRAINT_SCHEMA = kcu_pk.CONSTRAINT_SCHEMA
            AND rc.UNIQUE_CONSTRAINT_NAME = kcu_pk.CONSTRAINT_NAME
            AND kcu_fk.ORDINAL_POSITION = kcu_pk.ORDINAL_POSITION
        INNER JOIN ingested_tables it_child
            ON UPPER(kcu_fk.TABLE_SCHEMA) = it_child.SCHEMA_ONLY_KEY
            AND UPPER(REPLACE(kcu_fk.TABLE_NAME, '_', '')) = it_child.TABLE_ONLY_KEY
        INNER JOIN ingested_tables it_parent
            ON UPPER(kcu_pk.TABLE_SCHEMA) = it_parent.SCHEMA_ONLY_KEY
            AND UPPER(REPLACE(kcu_pk.TABLE_NAME, '_', '')) = it_parent.TABLE_ONLY_KEY
    ),
    columns_to_clear_fk AS (
        SELECT DISTINCT
            UPPER(fk.FK_SCHEMA) || '.' || 
            UPPER(REPLACE(fk.FK_TABLE, '_', '')) || '.' || 
            UPPER(REPLACE(fk.FK_COLUMN, '_', '')) as COLUMN_KEY,
            fk.CONSTRAINT_NAME
        FROM fk_relationships fk
        INNER JOIN composite_pk_tables cpt
            ON fk.PK_SCHEMA = cpt.TABLE_SCHEMA
            AND fk.PK_TABLE = cpt.TABLE_NAME
        WHERE fk.fk_column_count = 2  -- Only 2-column composite FKs
    )
    SELECT 
        COLUMN_KEY,
        'Was part of 2-column FK (' || CONSTRAINT_NAME || '), now replaced by surrogate FK' as CLEAR_NOTE
    FROM columns_to_clear_fk
) cleared_fk_cols
ON mc.COLUMN_KEY = cleared_fk_cols.COLUMN_KEY
WHEN MATCHED THEN UPDATE SET
    mc.IS_FOREIGN_KEY = FALSE,
    mc.FK_CONSTRAINT_NAME = NULL,
    mc.FK_PARENT_SCHEMA_KEY = NULL,
    mc.FK_PARENT_TABLE_KEY = NULL,
    mc.FK_PARENT_COLUMN_KEY = NULL,
    mc.FK_PARENT_FULL_KEY = NULL,
    mc.FK_ORDINAL_POSITION = NULL,
    mc.NOTES = COALESCE(mc.NOTES || '; ', '') || cleared_fk_cols.CLEAR_NOTE,
    mc.UPDATED_AT = CURRENT_TIMESTAMP(),
    mc.UPDATED_BY = CURRENT_USER();


-- ============================================================================
-- EXAMPLE QUERIES FOR MAINTENANCE
-- ============================================================================

-- View all staging column definitions for a specific table
/*
SELECT 
    TABLE_KEY,
    DBT_STG_COLUMN_NAME,
    DBT_STG_DATA_TYPE,
    DBT_STG_IS_NULLABLE,
    IS_PRIMARY_KEY,
    IS_FOREIGN_KEY,
    FK_PARENT_FULL_KEY,
    DBT_STG_TRANSFORMATION,
    COLUMN_SOURCE,
    DBT_STG_IS_EXCLUDED,
    IS_NEW_COLUMN,
    NOTES
FROM MS_RAW.DBT_META.META_COLUMNS
WHERE TABLE_KEY = 'DBO.TABLENAME'
ORDER BY 
    DBT_STG_IS_EXCLUDED,
    DBT_STG_ORDINAL_POSITION;
*/

-- View surrogate key replacement pattern for a child table
/*
WITH child_table_columns AS (
    SELECT 
        mc.TABLE_KEY as CHILD_TABLE,
        mc.DBT_STG_COLUMN_NAME,
        mc.IS_NEW_COLUMN,
        mc.IS_FOREIGN_KEY,
        mc.FK_PARENT_FULL_KEY,
        mc.COLUMN_SOURCE,
        mc.NOTES
    FROM MS_RAW.DBT_META.META_COLUMNS mc
    WHERE mc.TABLE_KEY = 'DBO.CHILDTABLE'
)
SELECT 
    CHILD_TABLE,
    DBT_STG_COLUMN_NAME,
    CASE 
        WHEN IS_NEW_COLUMN = TRUE AND IS_FOREIGN_KEY = TRUE 
            THEN 'NEW SURROGATE FK'
        WHEN IS_FOREIGN_KEY = FALSE AND NOTES LIKE '%part of 2-column FK%'
            THEN 'ORIGINAL FK COLUMN (FK cleared)'
        WHEN IS_FOREIGN_KEY = TRUE
            THEN 'REGULAR FK'
        ELSE 'REGULAR COLUMN'
    END as COLUMN_TYPE,
    IS_FOREIGN_KEY as STILL_FK,
    FK_PARENT_FULL_KEY,
    NOTES
FROM child_table_columns
ORDER BY 
    CASE 
        WHEN IS_NEW_COLUMN = TRUE AND IS_FOREIGN_KEY = TRUE THEN 1
        WHEN IS_FOREIGN_KEY = FALSE AND NOTES LIKE '%part of 2-column FK%' THEN 2
        ELSE 3
    END,
    DBT_STG_COLUMN_NAME;
*/

-- Summary: Surrogate key implementation impact
/*
SELECT 
    'Tables with composite PK getting surrogate key' as METRIC,
    COUNT(DISTINCT TABLE_KEY) as COUNT
FROM MS_RAW.DBT_META.META_COLUMNS
WHERE IS_PRIMARY_KEY = TRUE 
  AND IS_SURROGATE_KEY = TRUE
  AND IS_NEW_COLUMN = TRUE

UNION ALL

SELECT 
    'Child tables getting surrogate FK column' as METRIC,
    COUNT(DISTINCT TABLE_KEY) as COUNT
FROM MS_RAW.DBT_META.META_COLUMNS
WHERE IS_FOREIGN_KEY = TRUE 
  AND IS_SURROGATE_KEY = FALSE
  AND IS_NEW_COLUMN = TRUE
  AND COLUMN_SOURCE = 'SURROGATE'

UNION ALL

SELECT 
    'Original FK columns with FK metadata cleared' as METRIC,
    COUNT(*) as COUNT
FROM MS_RAW.DBT_META.META_COLUMNS
WHERE IS_FOREIGN_KEY = FALSE
  AND NOTES LIKE '%Was part of 2-column FK%';
*/

-- View complete FK transformation for specific parent-child relationship
/*
SELECT 
    parent.TABLE_KEY as PARENT_TABLE,
    parent.DBT_STG_COLUMN_NAME as PARENT_SURROGATE_KEY,
    parent.IS_PRIMARY_KEY as IS_PARENT_PK,
    '→' as REL,
    child.TABLE_KEY as CHILD_TABLE,
    child.DBT_STG_COLUMN_NAME as CHILD_COLUMN,
    child.IS_FOREIGN_KEY as IS_CHILD_FK,
    child.IS_NEW_COLUMN as IS_NEW,
    child.NOTES
FROM MS_RAW.DBT_META.META_COLUMNS parent
LEFT JOIN MS_RAW.DBT_META.META_COLUMNS child
    ON child.FK_PARENT_FULL_KEY = parent.COLUMN_KEY
WHERE parent.IS_PRIMARY_KEY = TRUE
  AND parent.IS_SURROGATE_KEY = TRUE
ORDER BY 
    parent.TABLE_KEY,
    child.TABLE_KEY,
    child.DBT_STG_COLUMN_NAME;
*/

-- Manually add a new business logic column
/*
INSERT INTO MS_RAW.DBT_META.META_COLUMNS (
    SCHEMA_ONLY_KEY,
    TABLE_ONLY_KEY,
    COLUMN_ONLY_KEY,
    TABLE_KEY,
    COLUMN_KEY,
    DBT_STG_COLUMN_NAME,
    DBT_STG_DATA_TYPE,
    DBT_STG_IS_NULLABLE,
    DBT_STG_TRANSFORMATION,
    COLUMN_SOURCE,
    IS_NEW_COLUMN,
    DBT_STG_DESCRIPTION,
    CREATED_BY
) VALUES (
    'DBO',
    'EMPLOYEES',
    'FULLNAME',
    'DBO.EMPLOYEES',
    'DBO.EMPLOYEES.FULLNAME',
    'full_name',
    'VARCHAR(500)',
    'YES',
    'CONCAT(first_name, '' '', last_name)',
    'BUSINESS',
    TRUE,
    'Concatenated full name from first and last name',
    CURRENT_USER()
);
*/

-- Exclude columns from staging (PII, deprecated, etc.)
/*
UPDATE MS_RAW.DBT_META.META_COLUMNS
SET 
    DBT_STG_IS_EXCLUDED = TRUE,
    NOTES = 'PII field - excluded from staging',
    UPDATED_AT = CURRENT_TIMESTAMP(),
    UPDATED_BY = CURRENT_USER()
WHERE COLUMN_KEY IN ('DBO.EMPLOYEES.SSN', 'DBO.EMPLOYEES.CREDITCARD');
*/

-- Change data type for staging (e.g., standardize dates)
/*
UPDATE MS_RAW.DBT_META.META_COLUMNS
SET 
    DBT_STG_DATA_TYPE = 'DATE',
    DBT_STG_TRANSFORMATION = 'TO_DATE(column_name)',
    UPDATED_AT = CURRENT_TIMESTAMP(),
    UPDATED_BY = CURRENT_USER()
WHERE MS_DATA_TYPE = 'datetime' 
  AND DBT_STG_DATA_TYPE = 'TIMESTAMP_NTZ';
*/
