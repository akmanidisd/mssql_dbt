-- ============================================================================
-- SP_VALIDATE_METADATA: Run data quality checks and validation
-- ============================================================================
-- Purpose: Validate STAGING_META_COLUMNS and STAGING_META_TABLES integrity
-- Returns: VARCHAR (validation results summary)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE MS_RAW.DBT_META.SP_VALIDATE_METADATA()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_result VARCHAR := '';
    v_step VARCHAR := '';
    v_errors NUMBER := 0;
    v_warnings NUMBER := 0;
BEGIN

    USE DATABASE MS_RAW;
    USE SCHEMA DBT_META;

    v_result := v_result || '=== METADATA VALIDATION REPORT ===\n\n';

    -- ========================================================================
    -- CHECK 1: All ingested tables have entries in STAGING_META_TABLES
    -- ========================================================================
    v_step := 'Checking table coverage';

    SELECT COUNT(*) INTO v_errors
    FROM MS_RAW.DBT_META.V_INGESTED_TABLES it
    WHERE it.TABLE_KEY NOT IN (
        SELECT TABLE_KEY FROM MS_RAW.DBT_META.STAGING_META_TABLES
    );

    IF (v_errors > 0) THEN
        v_result := v_result || 'ERROR: ' || v_errors || ' ingested tables missing from STAGING_META_TABLES\n';
    ELSE
        v_result := v_result || 'PASS: All ingested tables present in STAGING_META_TABLES\n';
    END IF;

    -- ========================================================================
    -- CHECK 2: All tables have at least one PK column
    -- ========================================================================
    v_step := 'Checking primary keys';

    SELECT COUNT(*) INTO v_warnings
    FROM MS_RAW.DBT_META.STAGING_META_TABLES
    WHERE PK_COLUMN_COUNT = 0;

    IF (v_warnings > 0) THEN
        v_result := v_result || 'WARNING: ' || v_warnings || ' tables have no primary key\n';
    ELSE
        v_result := v_result || 'PASS: All tables have primary keys\n';
    END IF;

    -- ========================================================================
    -- CHECK 3: All FK parent references exist
    -- ========================================================================
    v_step := 'Checking foreign key integrity';

    SELECT COUNT(*) INTO v_errors
    FROM MS_RAW.DBT_META.STAGING_META_COLUMNS smc
    WHERE smc.IS_FOREIGN_KEY = TRUE
      AND smc.FK_PARENT_KEY IS NOT NULL
      AND smc.FK_PARENT_KEY NOT IN (
          SELECT COLUMN_KEY FROM MS_RAW.DBT_META.STAGING_META_COLUMNS WHERE IS_PRIMARY_KEY = TRUE
      );

    IF (v_errors > 0) THEN
        v_result := v_result || 'ERROR: ' || v_errors || ' FKs reference non-existent parent columns\n';
    ELSE
        v_result := v_result || 'PASS: All FK references are valid\n';
    END IF;

    -- ========================================================================
    -- CHECK 4: No duplicate COLUMN_KEYs
    -- ========================================================================
    v_step := 'Checking for duplicates';

    SELECT COUNT(*) INTO v_errors
    FROM (
        SELECT COLUMN_KEY, COUNT(*) as cnt
        FROM MS_RAW.DBT_META.STAGING_META_COLUMNS
        GROUP BY COLUMN_KEY
        HAVING COUNT(*) > 1
    );

    IF (v_errors > 0) THEN
        v_result := v_result || 'ERROR: ' || v_errors || ' duplicate COLUMN_KEYs found\n';
    ELSE
        v_result := v_result || 'PASS: No duplicate COLUMN_KEYs\n';
    END IF;

    -- ========================================================================
    -- CHECK 5: All non-excluded columns have DBT_COLUMN_NAME
    -- ========================================================================
    v_step := 'Checking DBT column names';

    SELECT COUNT(*) INTO v_errors
    FROM MS_RAW.DBT_META.STAGING_META_COLUMNS
    WHERE DBT_IS_EXCLUDED = FALSE
      AND (DBT_COLUMN_NAME IS NULL OR DBT_COLUMN_NAME = '');

    IF (v_errors > 0) THEN
        v_result := v_result || 'ERROR: ' || v_errors || ' columns missing DBT_COLUMN_NAME\n';
    ELSE
        v_result := v_result || 'PASS: All columns have DBT_COLUMN_NAME\n';
    END IF;

    -- ========================================================================
    -- CHECK 6: Surrogate key naming convention
    -- ========================================================================
    v_step := 'Checking surrogate key naming';

    SELECT COUNT(*) INTO v_warnings
    FROM MS_RAW.DBT_META.STAGING_META_COLUMNS
    WHERE IS_SURROGATE_KEY = TRUE
      AND DBT_COLUMN_NAME NOT LIKE '%\\_id' ESCAPE '\\';

    IF (v_warnings > 0) THEN
        v_result := v_result || 'WARNING: ' || v_warnings || ' surrogate keys do not follow naming convention\n';
    ELSE
        v_result := v_result || 'PASS: All surrogate keys follow naming convention\n';
    END IF;

    -- ========================================================================
    -- SUMMARY STATISTICS
    -- ========================================================================
    v_result := v_result || '\n=== SUMMARY STATISTICS ===\n';

    SELECT
        COUNT(*) AS total_tables,
        SUM(IFF(HAS_SURROGATE_PK, 1, 0)) AS surrogate_pk_tables,
        SUM(IFF(HAS_NATURAL_PK, 1, 0)) AS natural_pk_tables,
        SUM(IFF(HAS_NO_PK, 1, 0)) AS no_pk_tables
    INTO v_result
    FROM MS_RAW.DBT_META.STAGING_META_TABLES;

    v_result := v_result || 'Tables: ' || v_result || '\n';

    SELECT
        COUNT(*) AS total_columns,
        SUM(IFF(IS_PRIMARY_KEY, 1, 0)) AS pk_columns,
        SUM(IFF(IS_FOREIGN_KEY, 1, 0)) AS fk_columns,
        SUM(IFF(IS_SURROGATE_KEY, 1, 0)) AS surrogate_columns,
        SUM(IFF(IS_NEW_COLUMN, 1, 0)) AS new_columns
    INTO v_result
    FROM MS_RAW.DBT_META.STAGING_META_COLUMNS;

    v_result := v_result || 'Columns: ' || v_result || '\n';

    -- ========================================================================
    -- FINAL VERDICT
    -- ========================================================================
    v_result := v_result || '\n=== VALIDATION RESULT ===\n';
    IF (v_errors = 0) THEN
        v_result := v_result || 'STATUS: PASSED (with ' || v_warnings || ' warnings)\n';
    ELSE
        v_result := v_result || 'STATUS: FAILED (' || v_errors || ' errors, ' || v_warnings || ' warnings)\n';
    END IF;

    v_result := v_result || 'Completed in ' || DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP()) || ' seconds\n';

    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR in step: ' || v_step || '\nError: ' || SQLERRM || '\nStack: ' || SQLCODE;
END;
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE MS_RAW.DBT_META.SP_VALIDATE_METADATA() TO ROLE SYSADMIN;

-- ============================================================================
-- Test execution
-- ============================================================================
-- CALL MS_RAW.DBT_META.SP_VALIDATE_METADATA();
