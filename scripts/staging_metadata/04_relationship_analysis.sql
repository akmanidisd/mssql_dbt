-- ============================================================================
-- SP_ANALYZE_RELATIONSHIPS: Analyze PK/FK relationships and plan surrogate keys
-- ============================================================================
-- Purpose: Create views for single-column PKs, composite PKs, FK relationships
--          and generate surrogate key implementation plan
-- Returns: VARCHAR (relationship statistics and success message)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE MS_RAW.DBT_META.SP_ANALYZE_RELATIONSHIPS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS

DECLARE
    v_start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_result VARCHAR := '';
    v_step VARCHAR := '';
    v_single_pk_count NUMBER := 0;
    v_composite_pk_count NUMBER := 0;
    v_fk_count NUMBER := 0;
BEGIN

    USE DATABASE MS_RAW;
    USE SCHEMA DBT_META;

    -- ========================================================================
    -- STEP 1: Create V_PRIMARY_KEYS view
    -- ========================================================================
    v_step := 'Creating V_PRIMARY_KEYS view';

    CREATE OR REPLACE VIEW MS_RAW.DBT_META.V_PRIMARY_KEYS AS
    SELECT
        sfc.TABLE_KEY,
        sfc.COLUMN_KEY,
        sfc.SF_TABLE_SCHEMA AS TABLE_SCHEMA,
        sfc.SF_TABLE_NAME   AS TABLE_NAME,
        sfc.SF_COLUMN_NAME  AS COLUMN_NAME,
        COUNT(SF_COLUMN_NAME) OVER (PARTITION BY sfc.TABLE_KEY) AS PK_COLUMNS,
        kcu.ORDINAL_POSITION
    FROM MS_RAW.MS_META.TABLE_CONSTRAINTS pc
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu
        ON kcu.TABLE_SCHEMA = pc.TABLE_SCHEMA
        AND kcu.TABLE_NAME = pc.TABLE_NAME
        AND kcu.CONSTRAINT_NAME = pc.CONSTRAINT_NAME
    INNER JOIN MS_RAW.DBT_META.SF_COLUMNS sfc
        ON kcu.TABLE_SCHEMA = sfc.MS_TABLE_SCHEMA
        AND kcu.TABLE_NAME = sfc.MS_TABLE_NAME
        AND kcu.COLUMN_NAME = sfc.MS_COLUMN_NAME
    WHERE pc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    ORDER BY sfc.COLUMN_KEY;

    SELECT COUNT(*) INTO v_pk_count
    FROM MS_RAW.DBT_META.V_PRIMARY_KEYS;

    v_result := v_result || 'PKs: ' || v_pk_count || '\n';

    -- ========================================================================
    -- STEP 3: Create V_FK_RELATIONSHIPS view
    -- ========================================================================
    v_step := 'Creating V_FK_RELATIONSHIPS view';

    CREATE OR REPLACE VIEW MS_RAW.DBT_META.V_FK_RELATIONSHIPS AS
    SELECT
        rc.CONSTRAINT_NAME,
        dbt_fk.TABLE_KEY AS CHILD_TABLE_KEY,
        dbt_fk.COLUMN_KEY AS CHILD_COLUMN_KEY,
        dbt_fk.SF_SCHEMA as CHILD_SCHEMA,
        dbt_fk.SF_TABLE as CHILD_TABLE,
        dbt_fk.SF_COLUMN as CHILD_COLUMN,
        kcu_fk.ORDINAL_POSITION as FK_ORDINAL,
        dbt_pk.TABLE_KEY AS PARENT_TABLE_KEY,
        dbt_pk.COLUMN_KEY AS PARENT_COLUMN_KEY,
        dbt_pk.SF_SCHEMA as PARENT_SCHEMA,
        dbt_pk.SF_TABLE as PARENT_TABLE,
        dbt_pk.SF_COLUMN as PARENT_COLUMN,
        kcu_pk.ORDINAL_POSITION as PK_ORDINAL,
        COUNT(*) OVER (PARTITION BY rc.CONSTRAINT_NAME) as fk_column_count
    FROM MS_RAW.MS_META.REFERENTIAL_CONSTRAINTS rc
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_fk
        ON rc.CONSTRAINT_SCHEMA = kcu_fk.CONSTRAINT_SCHEMA
        AND rc.CONSTRAINT_NAME = kcu_fk.CONSTRAINT_NAME
    INNER JOIN MS_RAW.MS_META.KEY_COLUMN_USAGE kcu_pk
        ON rc.UNIQUE_CONSTRAINT_SCHEMA = kcu_pk.CONSTRAINT_SCHEMA
        AND rc.UNIQUE_CONSTRAINT_NAME = kcu_pk.CONSTRAINT_NAME
        AND kcu_fk.ORDINAL_POSITION = kcu_pk.ORDINAL_POSITION
    INNER JOIN MS_RAW.DBT_META.STAGING_META_COLUMNS as dbt_fk
        ON  kcu_fk.TABLE_SCHEMA = dbt_fk.MS_SCHEMA
        AND kcu_fk.TABLE_NAME   = dbt_fk.MS_TABLE
        AND kcu_fk.COLUMN_NAME  = dbt_fk.MS_COLUMN
    INNER JOIN MS_RAW.DBT_META.STAGING_META_COLUMNS as dbt_pk
        ON  kcu_pk.TABLE_SCHEMA = dbt_pk.MS_SCHEMA
        AND kcu_pk.TABLE_NAME   = dbt_pk.MS_TABLE
        AND kcu_pk.COLUMN_NAME  = dbt_pk.MS_COLUMN
    ORDER BY CHILD_TABLE_KEY, PARENT_TABLE_KEY;

    SELECT COUNT(DISTINCT CONSTRAINT_NAME) INTO v_fk_count
    FROM MS_RAW.DBT_META.V_FK_RELATIONSHIPS;

    v_result := v_result || 'FK relationships: ' || v_fk_count || '\n';

    -- ========================================================================
    -- STEP 4: Create V_SURROGATE_KEY_PLAN view
    -- ========================================================================
    v_step := 'Creating V_SURROGATE_KEY_PLAN view';

    CREATE OR REPLACE VIEW MS_RAW.DBT_META.V_SURROGATE_KEY_PLAN AS
    -- Part A: Parent tables needing surrogate PKs
    SELECT
        TABLE_KEY,
        TABLE_SCHEMA,
        TABLE_NAME,
        TABLE_NAME || '_ID' AS NEW_COLUMN_NAME,
        'NUMBER(38,0)' AS NEW_DATA_TYPE,
        LISTAGG(COLUMN_NAME,' * 10^12 + ')
                WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS TRANSFORMATION,
        TRUE AS IS_PRIMARY_KEY,
        TRUE AS IS_SURROGATE_KEY,
        FALSE AS IS_FOREIGN_KEY,
        NULL AS FK_PARENT_TABLE_KEY,
        'SURROGATE' AS COLUMN_SOURCE,
        0 AS ORDINAL_POSITION,
        'Generated surrogate PK for composite PK table' AS NOTES
    FROM MS_RAW.DBT_META.V_PRIMARY_KEYS
    WHERE PK_COLUMNS = 2
    GROUP BY ALL

    UNION ALL
    -- PROBLEMATIC ?????????????????
    -- Part B: Child tables needing surrogate FKs
    SELECT
        fk.CHILD_TABLE_KEY AS TABLE_KEY,
        it_child.SCHEMA_ONLY_KEY,
        it_child.TABLE_ONLY_KEY,
        it_child.SF_TABLE_NAME,
        cpk.SURROGATE_KEY_NAME AS NEW_COLUMN_NAME,
        'NUMBER(38,0)' AS NEW_DATA_TYPE,
        NULL AS TRANSFORMATION,
        FALSE AS IS_PRIMARY_KEY,
        FALSE AS IS_SURROGATE_KEY,
        TRUE AS IS_FOREIGN_KEY,
        cpk.TABLE_KEY AS FK_PARENT_TABLE_KEY,
        'SURROGATE' AS COLUMN_SOURCE,
        999 AS ORDINAL_POSITION,
        'Surrogate FK to ' || cpk.TABLE_KEY || ' (replaces ' || fk.fk_column_count || '-column FK: ' || fk.CONSTRAINT_NAME || ')' AS NOTES
    FROM MS_RAW.DBT_META.V_FK_RELATIONSHIPS fk
    INNER JOIN MS_RAW.DBT_META.V_COMPOSITE_PKS cpk
        ON fk.PARENT_TABLE_KEY = cpk.TABLE_KEY
    INNER JOIN MS_RAW.DBT_META.V_INGESTED_TABLES it_child
        ON fk.CHILD_TABLE_KEY = it_child.TABLE_KEY
    WHERE fk.fk_column_count >= 2
    GROUP BY fk.CHILD_TABLE_KEY, it_child.SCHEMA_ONLY_KEY, it_child.TABLE_ONLY_KEY,
             it_child.SF_TABLE_NAME, cpk.SURROGATE_KEY_NAME, cpk.TABLE_KEY,
             fk.fk_column_count, fk.CONSTRAINT_NAME;

    v_result := v_result || 'Created surrogate key plan\n';

    -- ========================================================================
    -- STEP 5: Return Summary
    -- ========================================================================
    v_result := v_result || '\nRelationship analysis completed in '
        || DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP()) || ' seconds';

    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR in step: ' || v_step || '\nError: ' || SQLERRM || '\nStack: ' || SQLCODE;
END;
;

-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE MS_RAW.DBT_META.SP_ANALYZE_RELATIONSHIPS() TO ROLE SYSADMIN;

-- ============================================================================
-- Test execution
-- ============================================================================
-- CALL MS_RAW.DBT_META.SP_ANALYZE_RELATIONSHIPS();
