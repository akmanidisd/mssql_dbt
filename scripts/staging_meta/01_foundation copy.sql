-- ============================================================================
-- SP_SETUP_FOUNDATION: Initialize database structures and utilities
-- ============================================================================
-- Purpose: Create base database, schemas, metadata copies, utility functions
-- Returns: VARCHAR (success message or error details)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- Create Utility UDFs First (before the procedure)
-- ============================================================================

-- UDF: Generate standardized column key
CREATE OR REPLACE FUNCTION MS_RAW.DBT_META.UDF_GENERATE_COLUMN_KEY(
    p_schema VARCHAR,
    p_table VARCHAR,
    p_column VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS
$$
    UPPER(REPLACE(p_schema || '.' || p_table || '.' || p_column, '_', ''))
$$;

-- UDF: Generate standardized table key
CREATE OR REPLACE FUNCTION MS_RAW.DBT_META.UDF_GENERATE_TABLE_KEY(
    p_schema VARCHAR,
    p_table VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS
$$
    UPPER(REPLACE(p_schema || '.' || p_table, '_', ''))
$$;

-- UDF: Generate surrogate key name
CREATE OR REPLACE FUNCTION MS_RAW.DBT_META.UDF_GENERATE_SURROGATE_KEY_NAME(
    p_table_name VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS
$$
    LOWER(p_table_name) || '_id'
$$;

-- UDF: Classify column category based on name and type
CREATE OR REPLACE FUNCTION MS_RAW.DBT_META.UDF_CLASSIFY_COLUMN_CATEGORY(
    p_column_name VARCHAR,
    p_data_type VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS
$$
    CASE
        WHEN UPPER(p_column_name) LIKE '%\\_ID' ESCAPE '\\'
          OR UPPER(p_column_name) IN ('ID', 'KEY') THEN 'ID'
        WHEN p_data_type IN ('TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'TIMESTAMP_LTZ') THEN 'TIMESTAMP'
        WHEN p_data_type = 'DATE' THEN 'DATE'
        WHEN p_data_type = 'BOOLEAN'
          OR UPPER(p_column_name) LIKE 'IS\\_%' ESCAPE '\\'
          OR UPPER(p_column_name) LIKE 'HAS\\_%' ESCAPE '\\' THEN 'FLAG'
        WHEN p_data_type IN ('NUMBER', 'FLOAT', 'DECIMAL')
          AND UPPER(p_column_name) LIKE ANY ('%AMOUNT', '%PRICE', '%COST', '%TOTAL', '%QTY', '%QUANTITY') THEN 'MEASURE'
        WHEN p_data_type IN ('NUMBER', 'FLOAT', 'DECIMAL') THEN 'NUMERIC'
        WHEN p_data_type LIKE 'VARCHAR%' OR p_data_type = 'TEXT' THEN 'TEXT'
        ELSE 'DIMENSION'
    END
$$;

-- ============================================================================
-- Create Main Procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE MS_RAW.DBT_META.SP_SETUP_FOUNDATION()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    v_result VARCHAR := '';
    v_step VARCHAR := '';
BEGIN

    -- ========================================================================
    -- STEP 1: Create MS_RAW Database and Schemas
    -- ========================================================================
    v_step := 'Creating MS_RAW database and schemas';

    CREATE DATABASE IF NOT EXISTS MS_RAW
        COMMENT = 'MSSQL data migrated to Snowflake with metadata for dbt staging';

    USE DATABASE MS_RAW;

    -- Create data schemas (map from ROL_RAW REEDONLINE_* schemas)
    CREATE SCHEMA IF NOT EXISTS MS_RAW.DBO
        COMMENT = 'MSSQL DBO schema (from ROL_RAW.REEDONLINE_DBO and REEDONLINE_RESTRICTED)';

    CREATE SCHEMA IF NOT EXISTS MS_RAW.DUPLICATEJOBSERVICE
        COMMENT = 'MSSQL DUPLICATEJOBSERVICE schema';

    CREATE SCHEMA IF NOT EXISTS MS_RAW.JOBIMPORT
        COMMENT = 'MSSQL JOBIMPORT schema';

    CREATE SCHEMA IF NOT EXISTS MS_RAW.JOBS
        COMMENT = 'MSSQL JOBS schema';

    CREATE SCHEMA IF NOT EXISTS MS_RAW.RECRUITERJOBSTEXTKERNEL
        COMMENT = 'MSSQL RECRUITERJOBSTEXTKERNEL schema';

    v_result := v_result || 'Created data schemas (DBO, JOBS, etc.)\n';

    -- ========================================================================
    -- STEP 2: Create Metadata Schemas
    -- ========================================================================
    v_step := 'Creating metadata schemas';

    CREATE SCHEMA IF NOT EXISTS MS_RAW.MS_META
        COMMENT = 'MSSQL INFORMATION_SCHEMA metadata copies';

    CREATE SCHEMA IF NOT EXISTS MS_RAW.DBT_META
        COMMENT = 'dbt staging metadata and transformation tables';

    v_result := v_result || 'Created metadata schemas (MS_META, DBT_META)\n';

    -- ========================================================================
    -- STEP 3: Copy MSSQL Metadata from ROL_RAW
    -- ========================================================================
    v_step := 'Copying MSSQL metadata from ROL_RAW';

    -- COLUMNS: All column definitions
    CREATE OR REPLACE TABLE MS_RAW.MS_META.COLUMNS AS
    SELECT *
    FROM ROL_RAW.REEDONLINE_META.COLUMNS
    WHERE TABLE_SCHEMA IN ('dbo', 'DuplicateJobService', 'JobImport', 'Jobs', 'RecruiterJobsTextKernel');

    -- KEY_COLUMN_USAGE: PK and FK column references
    CREATE OR REPLACE TABLE MS_RAW.MS_META.KEY_COLUMN_USAGE AS
    SELECT *
    FROM ROL_RAW.REEDONLINE_META.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA IN ('dbo', 'DuplicateJobService', 'JobImport', 'Jobs', 'RecruiterJobsTextKernel');

    -- REFERENTIAL_CONSTRAINTS: FK relationship definitions
    CREATE OR REPLACE TABLE MS_RAW.MS_META.REFERENTIAL_CONSTRAINTS AS
    SELECT *
    FROM ROL_RAW.REEDONLINE_META.REFERENTIAL_CONSTRAINTS
    WHERE CONSTRAINT_SCHEMA IN ('dbo', 'DuplicateJobService', 'JobImport', 'Jobs', 'RecruiterJobsTextKernel');

    -- TABLES: Table catalog
    CREATE OR REPLACE TABLE MS_RAW.MS_META.TABLES AS
    SELECT *
    FROM ROL_RAW.REEDONLINE_META.TABLES
    WHERE TABLE_SCHEMA IN ('dbo', 'DuplicateJobService', 'JobImport', 'Jobs', 'RecruiterJobsTextKernel')
      AND TABLE_TYPE = 'BASE TABLE';

    -- TABLE_CONSTRAINTS: PK and FK constraint metadata
    CREATE OR REPLACE TABLE MS_RAW.MS_META.TABLE_CONSTRAINTS AS
    SELECT *
    FROM ROL_RAW.REEDONLINE_META.TABLE_CONSTRAINTS
    WHERE TABLE_SCHEMA IN ('dbo', 'DuplicateJobService', 'JobImport', 'Jobs', 'RecruiterJobsTextKernel')
      AND CONSTRAINT_TYPE IN ('PRIMARY KEY', 'FOREIGN KEY');

    v_result := v_result || 'Copied 5 MSSQL metadata tables to MS_META schema\n';

    -- ========================================================================
    -- STEP 4: Create Schema Mapping View
    -- ========================================================================
    v_step := 'Creating schema mapping view';

    CREATE OR REPLACE VIEW MS_RAW.DBT_META.V_SCHEMA_MAPPING AS
    SELECT
        'REEDONLINE_DBO' AS ROL_RAW_SCHEMA,
        'DBO' AS MS_RAW_SCHEMA,
        'dbo' AS MSSQL_SCHEMA
    UNION ALL
    SELECT 'REEDONLINE_RESTRICTED', 'DBO', 'dbo'
    UNION ALL
    SELECT 'REEDONLINE_DUPLICATEJOBSERVICE', 'DUPLICATEJOBSERVICE', 'DuplicateJobService'
    UNION ALL
    SELECT 'REEDONLINE_JOBIMPORT', 'JOBIMPORT', 'JobImport'
    UNION ALL
    SELECT 'REEDONLINE_JOBS', 'JOBS', 'Jobs'
    UNION ALL
    SELECT 'REEDONLINE_RECRUITERJOBSTEXTKERNEL', 'RECRUITERJOBSTEXTKERNEL', 'RecruiterJobsTextKernel';

    v_result := v_result || 'Created V_SCHEMA_MAPPING view\n';

    -- ========================================================================
    -- STEP 6: Create Audit Log Table
    -- ========================================================================
    v_step := 'Creating audit log table';

    CREATE TABLE IF NOT EXISTS MS_RAW.DBT_META.METADATA_BUILD_LOG (
        LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
        EXECUTION_START_TIME TIMESTAMP_NTZ NOT NULL,
        EXECUTION_END_TIME TIMESTAMP_NTZ,
        DURATION_SECONDS NUMBER(10,2),
        PROCEDURE_NAME VARCHAR(255) NOT NULL,
        REBUILD_FOUNDATION BOOLEAN,
        EXCLUDE_TABLES VARCHAR(1000),
        VALIDATION_PASSED BOOLEAN,
        STATUS VARCHAR(20) NOT NULL,  -- RUNNING, SUCCESS, FAILED
        RESULT_SUMMARY VARCHAR,
        ERROR_MESSAGE VARCHAR,
        TABLES_CREATED NUMBER,
        COLUMNS_CREATED NUMBER,
        EXECUTED_BY VARCHAR DEFAULT CURRENT_USER(),
        SNOWFLAKE_VERSION VARCHAR DEFAULT CURRENT_VERSION()
    )
    COMMENT = 'Audit trail for metadata pipeline executions';

    v_result := v_result || 'Created METADATA_BUILD_LOG table\n';

    -- ========================================================================
    -- STEP 7: Return Success Summary
    -- ========================================================================
    v_result := v_result || '\nFoundation setup completed in '
        || DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP()) || ' seconds';

    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR in step: ' || v_step || '\nError: ' || SQLERRM || '\nStack: ' || SQLCODE;
END;
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE MS_RAW.DBT_META.SP_SETUP_FOUNDATION() TO ROLE SYSADMIN;

-- ============================================================================
-- Test execution
-- ============================================================================
-- CALL MS_RAW.DBT_META.SP_SETUP_FOUNDATION();
