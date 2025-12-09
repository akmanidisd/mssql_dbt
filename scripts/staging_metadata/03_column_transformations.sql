-- ============================================================================
-- SP_NORMALIZE_COLUMNS: Apply column transformations and business rules
-- ============================================================================
-- Purpose: Create transformation rules for timestamp normalization, naming
--          conventions, derived columns, and type conversions
-- Returns: VARCHAR (transformation count and success message)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE MS_RAW.DBT_META.SP_NORMALIZE_COLUMNS()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var v_start_time = new Date();
    var v_result = '';
    var v_step = '';
    var v_transform_count = 0;

    try {
        // ====================================================================
        // STEP 1: Create COLUMN_TRANSFORMATIONS table
        // ====================================================================
        v_step = 'Creating COLUMN_TRANSFORMATIONS table';

        snowflake.execute({sqlText: `
            CREATE OR REPLACE TRANSIENT TABLE MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS (
                COLUMN_KEY VARCHAR,
                TABLE_KEY VARCHAR NOT NULL,
                SF_COLUMN_NAME VARCHAR NOT NULL,
                SF_DATA_TYPE VARCHAR NOT NULL,
                MS_DATA_TYPE VARCHAR,

                -- Target staging definitions
                DBT_COLUMN_NAME VARCHAR,
                DBT_DATA_TYPE VARCHAR,
                DBT_TRANSFORMATION VARCHAR,

                -- Transformation metadata
                TRANSFORMATION_REASON VARCHAR,
                COLUMN_SOURCE VARCHAR DEFAULT 'MSSQL',
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            CLUSTER BY (TABLE_KEY)
            COMMENT = 'Column transformation rules for staging models'
        `});

        // ====================================================================
        // STEP 2: Analyze timestamp columns for DATE vs TIMESTAMP_NTZ
        // ====================================================================
        v_step = 'Analyzing timestamp columns';

        // Create analysis table for TIMESTAMP_TZ columns
        snowflake.execute({sqlText: `
            CREATE OR REPLACE TRANSIENT TABLE MS_RAW.DBT_META.TIMESTAMP_ANALYSIS AS
            SELECT
                COLUMN_KEY,
                TABLE_KEY,
                SF_TABLE_SCHEMA,
                SF_TABLE_NAME,
                SF_COLUMN_NAME,
                SF_DATA_TYPE,
                MS_DATA_TYPE,
                -- Mark as needing analysis
                TRUE AS NEEDS_ANALYSIS
            FROM MS_RAW.DBT_META.SF_COLUMNS
            WHERE SF_DATA_TYPE IN ('TIMESTAMP_TZ', 'TIMESTAMP_LTZ')
              AND TABLE_KEY IN (SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_INGESTED_TABLES)
        `});

        v_result += 'Created TIMESTAMP_ANALYSIS table\\n';

        // ====================================================================
        // STEP 3: Insert base transformations
        // ====================================================================
        v_step = 'Inserting base transformations';

        // Rule 2: DATE columns - remove DATE_ prefix and _DATE suffix
        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS
            SELECT
                COLUMN_KEY,
                TABLE_KEY,
                SF_COLUMN_NAME,
                SF_DATA_TYPE,
                MS_DATA_TYPE,
                REPLACE(REPLACE(SF_COLUMN_NAME, 'DATE_', ''), '_DATE', '') AS DBT_COLUMN_NAME,
                'DATE' AS DBT_DATA_TYPE,
                NULL AS DBT_TRANSFORMATION,
                'Date column name normalization' AS TRANSFORMATION_REASON,
                'MSSQL' AS COLUMN_SOURCE,
                CURRENT_TIMESTAMP()
            FROM MS_RAW.DBT_META.SF_COLUMNS
            WHERE COLUMN_KEY NOT IN (SELECT COLUMN_KEY FROM MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS)
              AND SF_DATA_TYPE = 'DATE'
              AND (SF_COLUMN_NAME LIKE 'DATE\\_%' ESCAPE '\\\\'
                   OR SF_COLUMN_NAME LIKE '%\\_DATE\\_%' ESCAPE '\\\\'
                   OR SF_COLUMN_NAME LIKE '%\\_DATE' ESCAPE '\\\\')
              AND TABLE_KEY IN (SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_INGESTED_TABLES)
        `});

        // Rule 3: TIMESTAMP_NTZ columns ending in _ON → change to _AT
        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS
            SELECT
                COLUMN_KEY,
                TABLE_KEY,
                SF_COLUMN_NAME,
                SF_DATA_TYPE,
                MS_DATA_TYPE,
                REPLACE(SF_COLUMN_NAME, '_ON', '_AT') AS DBT_COLUMN_NAME,
                'TIMESTAMP_NTZ' AS DBT_DATA_TYPE,
                NULL AS DBT_TRANSFORMATION,
                'Timestamp naming convention: _ON -> _AT' AS TRANSFORMATION_REASON,
                'MSSQL' AS COLUMN_SOURCE,
                CURRENT_TIMESTAMP()
            FROM MS_RAW.DBT_META.SF_COLUMNS
            WHERE COLUMN_KEY NOT IN (SELECT COLUMN_KEY FROM MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS)
              AND SF_DATA_TYPE = 'TIMESTAMP_NTZ'
              AND SF_COLUMN_NAME LIKE '%\\_ON' ESCAPE '\\\\'
              AND TABLE_KEY IN (SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_INGESTED_TABLES)
        `});

        // Rule 4: TIMESTAMP_NTZ columns not ending in _AT → append _AT
        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS
            SELECT
                COLUMN_KEY,
                TABLE_KEY,
                SF_COLUMN_NAME,
                SF_DATA_TYPE,
                MS_DATA_TYPE,
                SF_COLUMN_NAME || '_AT' AS DBT_COLUMN_NAME,
                'TIMESTAMP_NTZ' AS DBT_DATA_TYPE,
                NULL AS DBT_TRANSFORMATION,
                'Timestamp naming convention: append _AT' AS TRANSFORMATION_REASON,
                'MSSQL' AS COLUMN_SOURCE,
                CURRENT_TIMESTAMP()
            FROM MS_RAW.DBT_META.SF_COLUMNS
            WHERE COLUMN_KEY NOT IN (SELECT COLUMN_KEY FROM MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS)
              AND SF_DATA_TYPE = 'TIMESTAMP_NTZ'
              AND SF_COLUMN_NAME NOT LIKE '%\\_AT' ESCAPE '\\\\'
              AND TABLE_KEY IN (SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_INGESTED_TABLES)
        `});

        // Rule 5: Special case - FROM/TO → HISTORY_FROM_DATE/HISTORY_TO_DATE
        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS
            SELECT
                COLUMN_KEY,
                TABLE_KEY,
                SF_COLUMN_NAME,
                SF_DATA_TYPE,
                MS_DATA_TYPE,
                CASE
                    WHEN SF_COLUMN_NAME = 'FROM' THEN 'HISTORY_FROM_DATE'
                    WHEN SF_COLUMN_NAME = 'TO' THEN 'HISTORY_TO_DATE'
                END AS DBT_COLUMN_NAME,
                'DATE' AS DBT_DATA_TYPE,
                NULL AS DBT_TRANSFORMATION,
                'Reserved keyword renamed' AS TRANSFORMATION_REASON,
                'MSSQL' AS COLUMN_SOURCE,
                CURRENT_TIMESTAMP()
            FROM MS_RAW.DBT_META.SF_COLUMNS
            WHERE COLUMN_KEY NOT IN (SELECT COLUMN_KEY FROM MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS)
              AND SF_COLUMN_NAME IN ('FROM', 'TO')
              AND SF_DATA_TYPE = 'DATE'
              AND TABLE_KEY IN (SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_INGESTED_TABLES)
        `});

        // Rule 6: All other columns - no transformation (keep as-is)
        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS
            SELECT
                COLUMN_KEY,
                TABLE_KEY,
                SF_COLUMN_NAME,
                SF_DATA_TYPE,
                MS_DATA_TYPE,
                SF_COLUMN_NAME AS DBT_COLUMN_NAME,
                SF_DATA_TYPE AS DBT_DATA_TYPE,
                NULL AS DBT_TRANSFORMATION,
                'No transformation needed' AS TRANSFORMATION_REASON,
                'MSSQL' AS COLUMN_SOURCE,
                CURRENT_TIMESTAMP()
            FROM MS_RAW.DBT_META.SF_COLUMNS
            WHERE COLUMN_KEY NOT IN (SELECT COLUMN_KEY FROM MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS)
              AND TABLE_KEY IN (SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_INGESTED_TABLES)
        `});

        // ====================================================================
        // STEP 4: Add derived column transformations
        // ====================================================================
        v_step = 'Adding derived column transformations';

        // DLT load ID transformation
        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS
            SELECT
                REPLACE(COLUMN_KEY, '_DLT_LOAD_ID', '_DLT_LOADED_AT') AS COLUMN_KEY,
                TABLE_KEY,
                '_DLT_LOAD_ID' AS SF_COLUMN_NAME,
                'VARCHAR' AS SF_DATA_TYPE,
                NULL AS MS_DATA_TYPE,
                '_DLT_LOADED_AT' AS DBT_COLUMN_NAME,
                'TIMESTAMP_NTZ' AS DBT_DATA_TYPE,
                'CAST(_DLT_LOAD_ID AS NUMBER(18,7))::TIMESTAMP_NTZ(9)' AS DBT_TRANSFORMATION,
                'Convert dlt load ID to timestamp' AS TRANSFORMATION_REASON,
                'DERIVED' AS COLUMN_SOURCE,
                CURRENT_TIMESTAMP()
            FROM MS_RAW.DBT_META.SF_COLUMNS
            WHERE SF_COLUMN_NAME = '_DLT_LOAD_ID'
              AND TABLE_KEY IN (SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_INGESTED_TABLES)
        `});

        // Row iteration transformations
        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS
            SELECT
                REPLACE(COLUMN_KEY, UPPER(SF_COLUMN_NAME), UPPER(SF_COLUMN_NAME) || 'NUM') AS COLUMN_KEY,
                TABLE_KEY,
                SF_COLUMN_NAME,
                SF_DATA_TYPE,
                MS_DATA_TYPE,
                CASE
                    WHEN SF_COLUMN_NAME = 'TIME_STAMP' THEN 'ROW_ITERATION_NUM'
                    WHEN SF_COLUMN_NAME = 'SJS_ROW_ITERATION' THEN 'SJS_ROW_ITERATION_NUM'
                    WHEN SF_COLUMN_NAME = 'ROW_ITERATION' THEN 'ROW_ITERATION_NUM'
                END AS DBT_COLUMN_NAME,
                'NUMBER' AS DBT_DATA_TYPE,
                'TO_NUMBER(TO_VARCHAR(' || SF_COLUMN_NAME || '), \\'XXXXXXXXXXXXXXXX\\')' AS DBT_TRANSFORMATION,
                'Convert row iteration to number' AS TRANSFORMATION_REASON,
                'DERIVED' AS COLUMN_SOURCE,
                CURRENT_TIMESTAMP()
            FROM MS_RAW.DBT_META.SF_COLUMNS
            WHERE SF_COLUMN_NAME IN ('TIME_STAMP', 'SJS_ROW_ITERATION', 'ROW_ITERATION')
              AND TABLE_KEY IN (SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_INGESTED_TABLES)
        `});

        // ====================================================================
        // STEP 5: Get transformation count and return
        // ====================================================================
        var count_rs = snowflake.execute({
            sqlText: "SELECT COUNT(*) AS CNT FROM MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS"
        });
        if (count_rs.next()) {
            v_transform_count = count_rs.getColumnValue('CNT');
        }

        v_result += 'Created ' + v_transform_count + ' column transformations\\n';

        var duration = (new Date() - v_start_time) / 1000;
        v_result += '\\nColumn normalization completed in ' + duration.toFixed(2) + ' seconds';

        return v_result;

    } catch (err) {
        return 'ERROR in step: ' + v_step + '\\nError: ' + err.message;
    }
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE MS_RAW.DBT_META.SP_NORMALIZE_COLUMNS() TO ROLE SYSADMIN;

-- ============================================================================
-- Test execution
-- ============================================================================
-- CALL MS_RAW.DBT_META.SP_NORMALIZE_COLUMNS();
