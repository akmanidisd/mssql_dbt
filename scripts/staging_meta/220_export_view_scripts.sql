-- ================================================================
-- Script: 20_export_view_scripts.sql
-- Purpose: Export individual view scripts to files
-- Dependencies: Run 19_generate_view_scripts.sql first
-- ================================================================

-- ================================================================
-- OPTION 1: Get DDL for all views by schema
-- Copy/paste these results to create individual files
-- ================================================================

-- REEDONLINE_DBO Schema
SELECT
    '-- FILE: ' || FILE_PATH || '\n' ||
    DDL_SCRIPT || '\n\n' AS SCRIPT_OUTPUT
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
WHERE SF_TABLE_SCHEMA = 'REEDONLINE_DBO'
ORDER BY NEW_TABLE_NAME;

-- REEDONLINE_DUPLICATEJOBSERVICE Schema
SELECT
    '-- FILE: ' || FILE_PATH || '\n' ||
    DDL_SCRIPT || '\n\n' AS SCRIPT_OUTPUT
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
WHERE SF_TABLE_SCHEMA = 'REEDONLINE_DUPLICATEJOBSERVICE'
ORDER BY NEW_TABLE_NAME;

-- REEDONLINE_JOBIMPORT Schema
SELECT
    '-- FILE: ' || FILE_PATH || '\n' ||
    DDL_SCRIPT || '\n\n' AS SCRIPT_OUTPUT
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
WHERE SF_TABLE_SCHEMA = 'REEDONLINE_JOBIMPORT'
ORDER BY NEW_TABLE_NAME;

-- REEDONLINE_JOBS Schema
SELECT
    '-- FILE: ' || FILE_PATH || '\n' ||
    DDL_SCRIPT || '\n\n' AS SCRIPT_OUTPUT
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
WHERE SF_TABLE_SCHEMA = 'REEDONLINE_JOBS'
ORDER BY NEW_TABLE_NAME;

-- ================================================================
-- OPTION 2: Get a single combined deployment script per schema
-- ================================================================

-- Combined script for REEDONLINE_DBO
SELECT
    '-- ================================================================\n' ||
    '-- Combined View Deployment Script for REEDONLINE_DBO\n' ||
    '-- Generated: ' || CURRENT_TIMESTAMP()::VARCHAR || '\n' ||
    '-- Total Views: ' || COUNT(*) || '\n' ||
    '-- ================================================================\n\n' ||
    'USE ROLE ACCOUNTADMIN;\n\n' ||
    LISTAGG(DDL_SCRIPT, '\n') WITHIN GROUP (ORDER BY NEW_TABLE_NAME) AS COMBINED_SCRIPT
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
WHERE SF_TABLE_SCHEMA = 'REEDONLINE_DBO';

-- ================================================================
-- OPTION 3: Create a master deployment script for all schemas
-- ================================================================

SELECT
    '-- ================================================================\n' ||
    '-- Master View Deployment Script\n' ||
    '-- Generated: ' || CURRENT_TIMESTAMP()::VARCHAR || '\n' ||
    '-- Total Views: ' || COUNT(*) || '\n' ||
    '-- ================================================================\n\n' ||
    'USE ROLE ACCOUNTADMIN;\n\n' ||
    LISTAGG(
        '-- Schema: ' || SF_TABLE_SCHEMA || ' | Table: ' || NEW_TABLE_NAME || '\n' ||
        DDL_SCRIPT,
        '\n'
    ) WITHIN GROUP (ORDER BY SF_TABLE_SCHEMA, NEW_TABLE_NAME) AS MASTER_DEPLOYMENT_SCRIPT
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS;

-- ================================================================
-- OPTION 4: Validation queries
-- ================================================================

-- List all views to be created
SELECT
    ROW_NUMBER() OVER (ORDER BY SF_TABLE_SCHEMA, NEW_TABLE_NAME) AS VIEW_NUMBER,
    SF_TABLE_SCHEMA,
    SF_TABLE_NAME AS SOURCE_TABLE,
    NEW_TABLE_NAME AS VIEW_NAME,
    'MS_RAW.' || SF_TABLE_SCHEMA || '.STG_' || NEW_TABLE_NAME AS FULL_VIEW_NAME,
    FILE_PATH
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
ORDER BY SF_TABLE_SCHEMA, NEW_TABLE_NAME;

-- Count by schema
SELECT
    SF_TABLE_SCHEMA,
    COUNT(*) AS VIEW_COUNT
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
GROUP BY SF_TABLE_SCHEMA
ORDER BY SF_TABLE_SCHEMA;

-- ================================================================
-- OPTION 5: Sample views for testing
-- ================================================================

-- Get script for USER table (common table for testing)
SELECT DDL_SCRIPT
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
WHERE NEW_TABLE_NAME = 'USER';

-- Get script for JOB table (common table for testing)
SELECT DDL_SCRIPT
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
WHERE NEW_TABLE_NAME = 'JOB';

-- ================================================================
-- OPTION 6: Column details for a specific view
-- ================================================================

-- Show column ordering for USER table
SELECT
    COLUMN_ORDER,
    CASE COLUMN_CATEGORY
        WHEN 1 THEN '1_PK'
        WHEN 2 THEN '2_FK'
        WHEN 3 THEN '3_OTHER_ID'
        WHEN 4 THEN '4_DATE_TIMESTAMP'
        WHEN 5 THEN '5_BOOLEAN'
        WHEN 6 THEN '6_TEXT'
        WHEN 7 THEN '7_NUMERIC'
        WHEN 8 THEN '8_ROW_DLT'
        ELSE '9_OTHER'
    END AS CATEGORY,
    SF_COLUMN_NAME,
    NEW_COLUMN_NAME,
    DATA_TYPE,
    NEW_COLUMN_TYPE,
    COLUMN_SELECT_STATEMENT
FROM MS_RAW.STG_META.V_COLUMN_CLASSIFICATION
WHERE NEW_TABLE_NAME = 'USER'
ORDER BY COLUMN_ORDER;
