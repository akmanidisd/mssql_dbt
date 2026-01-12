-- ================================================================
-- Script: 19_generate_view_scripts.sql
-- Purpose: Generate CREATE VIEW scripts for staging tables
-- Output: DDL scripts organized by schema
-- ================================================================

-- ================================================================
-- STEP 1: Create helper view to classify columns by category
-- ================================================================
CREATE OR REPLACE VIEW MS_RAW.STG_META.V_COLUMN_CLASSIFICATION AS
WITH base_columns AS (
    SELECT
        sfc.SF_TABLE_SCHEMA,
        sfc.SF_TABLE_NAME,
        sfc.SF_COLUMN_NAME,
        sfc.NEW_COLUMN_NAME,
        sfc.NEW_COLUMN_TYPE,
        sfc.NEW_COLUMN_EXPRESSION,
        sfc.DATA_TYPE,
        sft.NEW_TABLE_NAME,
        sft.NEW_PRIMARY_KEY_NAME,
        sft.PK_COLUMNS,
        -- Check if this column is the PK
        CASE
            WHEN sfc.NEW_COLUMN_NAME = sft.NEW_PRIMARY_KEY_NAME THEN 1
            ELSE 0
        END AS IS_PK,
        -- Check if this column is an FK
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM MS_RAW.STG_META.T_SECONDARY_KEYS sk
                WHERE sk.FK_TABLE_SCHEMA = sfc.SF_TABLE_SCHEMA
                  AND sk.FK_TABLE_NAME = sfc.SF_TABLE_NAME
                  AND sk.FK_NEW_COLUMN_NAME = sfc.NEW_COLUMN_NAME
            ) THEN 1
            ELSE 0
        END AS IS_FK,
        -- Determine column category for ordering
        CASE
            WHEN sfc.NEW_COLUMN_NAME = sft.NEW_PRIMARY_KEY_NAME THEN 1
            WHEN EXISTS (
                SELECT 1
                FROM MS_RAW.STG_META.T_SECONDARY_KEYS sk
                WHERE sk.FK_TABLE_SCHEMA = sfc.SF_TABLE_SCHEMA
                  AND sk.FK_TABLE_NAME = sfc.SF_TABLE_NAME
                  AND sk.FK_NEW_COLUMN_NAME = sfc.NEW_COLUMN_NAME
            ) THEN 2
            WHEN sfc.NEW_COLUMN_NAME != sft.NEW_PRIMARY_KEY_NAME
                 AND ENDSWITH(sfc.NEW_COLUMN_NAME, '_ID') 
                 AND sfc.DATA_TYPE IN ('NUMBER', 'NUMERIC', 'INTEGER') THEN 3
            WHEN sfc.NEW_COLUMN_TYPE IN ('DATE', 'TIMESTAMP_NTZ') 
              OR sfc.DATA_TYPE IN ('DATE', 'TIMESTAMP_NTZ') THEN 4
            WHEN sfc.DATA_TYPE IN ('BOOLEAN') THEN 5
            WHEN sfc.DATA_TYPE IN ('TEXT', 'VARCHAR') 
             AND NOT sfc.SF_COLUMN_NAME ILIKE ANY ('_DLT%') THEN 6
            WHEN sfc.DATA_TYPE IN ('NUMBER', 'NUMERIC', 'INTEGER', 'FLOAT', 'DECIMAL') THEN 7
            WHEN sfc.SF_COLUMN_NAME ILIKE ANY ('_DLT%', '%ROW_ITERATION%', 'TIME_STAMP') THEN 8
            ELSE 9
        END AS COLUMN_CATEGORY,
        CASE
            WHEN sfc.NEW_COLUMN_EXPRESSION IS NOT NULL THEN sfc.NEW_COLUMN_EXPRESSION
            ELSE sfc.SF_COLUMN_NAME
        END AS COLUMN_EXPRESSION
    FROM MS_RAW.STG_META.SF_COLUMNS sfc
    INNER JOIN MS_RAW.STG_META.SF_TABLES sft
        ON sfc.SF_TABLE_SCHEMA = sft.SF_TABLE_SCHEMA
       AND sfc.SF_TABLE_NAME = sft.SF_TABLE_NAME
    WHERE sft.SF_TABLE_NAME != 'PRICEBOOK_ENTRY'
)
SELECT
    SF_TABLE_SCHEMA,
    SF_TABLE_NAME,
    SF_COLUMN_NAME,
    NEW_COLUMN_NAME,
    NEW_COLUMN_TYPE,
    NEW_COLUMN_EXPRESSION,
    DATA_TYPE,
    NEW_TABLE_NAME,
    NEW_PRIMARY_KEY_NAME,
    IS_PK,
    IS_FK,
    COLUMN_CATEGORY,
    COLUMN_EXPRESSION,
    -- Generate the column definition for SELECT statement
    CASE
        WHEN NEW_COLUMN_EXPRESSION IS NOT NULL AND NEW_COLUMN_TYPE IS NOT NULL THEN
            REPLACE(NEW_COLUMN_EXPRESSION, '{COLUMN_NAME}', SF_COLUMN_NAME) || ' AS ' || NEW_COLUMN_NAME
        WHEN NEW_COLUMN_TYPE IS NOT NULL AND CONTAINS(NEW_COLUMN_TYPE, '::') THEN
            SF_COLUMN_NAME || '::' || NEW_COLUMN_TYPE || ' AS ' || NEW_COLUMN_NAME
        WHEN NEW_COLUMN_TYPE IS NOT NULL AND CONTAINS(NEW_COLUMN_TYPE, 'TO_NUMBER') THEN
            REPLACE(NEW_COLUMN_TYPE, '{COLUMN_NAME}', SF_COLUMN_NAME) || ' AS ' || NEW_COLUMN_NAME
        WHEN NEW_COLUMN_NAME != SF_COLUMN_NAME THEN
            SF_COLUMN_NAME || ' AS ' || NEW_COLUMN_NAME
        ELSE
            SF_COLUMN_NAME
    END AS COLUMN_SELECT_STATEMENT,
    ROW_NUMBER() OVER (
        PARTITION BY SF_TABLE_SCHEMA, SF_TABLE_NAME
        ORDER BY COLUMN_CATEGORY, NEW_COLUMN_NAME
    ) AS COLUMN_ORDER
FROM base_columns
ORDER BY SF_TABLE_SCHEMA, SF_TABLE_NAME, COLUMN_CATEGORY, NEW_COLUMN_NAME;

-- ================================================================
-- STEP 2: Generate the CREATE VIEW scripts
-- ================================================================
CREATE OR REPLACE VIEW MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS AS
WITH column_rows AS (
    SELECT
        SF_TABLE_SCHEMA,
        SF_TABLE_NAME,
        NEW_TABLE_NAME,
        COLUMN_CATEGORY,
        COLUMN_SELECT_STATEMENT,
        COLUMN_ORDER * 10 + 5 AS NEW_COLUMN_ORDER
    FROM MS_RAW.STG_META.V_COLUMN_CLASSIFICATION 
)
, column_groups AS (
    SELECT
        SF_TABLE_SCHEMA,
        SF_TABLE_NAME,
        NEW_TABLE_NAME,
        COLUMN_CATEGORY,
        ' -- Category: ' || COLUMN_CATEGORY || ' ' ||
        CASE COLUMN_CATEGORY
            WHEN 1 THEN 'PRIMARY KEY'
            WHEN 2 THEN 'SECONDARY KEY'
            WHEN 3 THEN '_ID'
            WHEN 4 THEN 'DATE/TIMESTAMP'
            WHEN 5 THEN 'BOOLEAN'
            WHEN 6 THEN 'CHAR/VARCHAR'
            WHEN 7 THEN 'NUMERIC'
            WHEN 8 THEN 'META'
            WHEN 9 THEN 'OTHER/UNKNOWN'
            ELSE '??????????'
         END
        AS COLUMN_SELECT_STATEMENT,
        MIN(NEW_COLUMN_ORDER - 5) AS NEW_COLUMN_ORDER
    FROM column_rows
    GROUP BY ALL
)
, column_all AS (
    SELECT
        SF_TABLE_SCHEMA,
        SF_TABLE_NAME,
        NEW_TABLE_NAME,
        COLUMN_CATEGORY,
        COLUMN_SELECT_STATEMENT,
        NEW_COLUMN_ORDER
    FROM column_rows
    UNION ALL
    SELECT
        SF_TABLE_SCHEMA,
        SF_TABLE_NAME,
        NEW_TABLE_NAME,
        COLUMN_CATEGORY,
        COLUMN_SELECT_STATEMENT,
        NEW_COLUMN_ORDER
    FROM column_groups
)
, column_lists AS (
    SELECT
        SF_TABLE_SCHEMA,
        SF_TABLE_NAME,
        NEW_TABLE_NAME,
        LISTAGG(
            '    ' || COLUMN_SELECT_STATEMENT || -- ' ' || COLUMN_CATEGORY || ' ' || NEW_COLUMN_ORDER,
            ',\n'
        ) WITHIN GROUP (ORDER BY NEW_COLUMN_ORDER) AS COLUMN_LIST
    FROM column_all
    GROUP BY SF_TABLE_SCHEMA, SF_TABLE_NAME, NEW_TABLE_NAME
)
SELECT
    SF_TABLE_SCHEMA,
    SF_TABLE_NAME,
    NEW_TABLE_NAME,
    'scripts/staging_snowflake/' || SF_TABLE_SCHEMA || '/STG_' || NEW_TABLE_NAME || '.sql' AS FILE_PATH,
    '-- ================================================================\n' ||
    '-- View: MS_RAW.' || SF_TABLE_SCHEMA || '.STG_' || NEW_TABLE_NAME || '\n' ||
    '-- Source: MS_RAW.' || SF_TABLE_SCHEMA || '.' || SF_TABLE_NAME || '\n' ||
    '-- Generated: ' || CURRENT_TIMESTAMP()::VARCHAR || '\n' ||
    '-- ================================================================\n\n' ||
    'CREATE OR REPLACE VIEW MS_RAW.' || SF_TABLE_SCHEMA || '.STG_' || NEW_TABLE_NAME || ' AS\n' ||
    'SELECT\n' ||
    COLUMN_LIST || '\n' ||
    'FROM ROL_RAW.' || SF_TABLE_SCHEMA || '.' || SF_TABLE_NAME || '\n' ||
    ';\n'
--    '-- Grant permissions\n' ||
--    'GRANT SELECT ON VIEW MS_RAW.' || SF_TABLE_SCHEMA || '.STG_' || NEW_TABLE_NAME || ' TO ROLE ACCOUNTADMIN;\n'
     AS DDL_SCRIPT
FROM column_lists
ORDER BY SF_TABLE_SCHEMA, NEW_TABLE_NAME;

-- ================================================================
-- STEP 3: Query to review generated scripts
-- ================================================================
SELECT
    SF_TABLE_SCHEMA,
    NEW_TABLE_NAME,
    FILE_PATH,
    LENGTH(DDL_SCRIPT) AS SCRIPT_LENGTH
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
ORDER BY SF_TABLE_SCHEMA, NEW_TABLE_NAME;

-- ================================================================
-- STEP 4: Sample output - view a specific script
-- ================================================================
-- Uncomment to see a sample script:
-- SELECT DDL_SCRIPT
-- FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
-- WHERE NEW_TABLE_NAME = 'USER'
-- LIMIT 1;

-- ================================================================
-- STEP 5: Export scripts by schema
-- ================================================================
-- To get all scripts for a specific schema:
SELECT
-- DDL_SCRIPT
    LISTAGG(DDL_SCRIPT, '\n') WITHIN GROUP (ORDER BY NEW_TABLE_NAME) AS "--DDL_SCRIPT"
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
WHERE SF_TABLE_SCHEMA = 'REEDONLINE_DBO'
-- ORDER BY NEW_TABLE_NAME
;

-- ================================================================
-- STEP 6: Statistics by schema
-- ================================================================
SELECT
    SF_TABLE_SCHEMA,
    COUNT(*) AS TABLE_COUNT,
    COUNT(DISTINCT NEW_TABLE_NAME) AS UNIQUE_TABLE_COUNT,
    AVG(LENGTH(DDL_SCRIPT)) AS AVG_SCRIPT_LENGTH
FROM MS_RAW.STG_META.V_GENERATED_VIEW_SCRIPTS
GROUP BY SF_TABLE_SCHEMA
ORDER BY SF_TABLE_SCHEMA;

-- ================================================================
-- STEP 7: Column category breakdown
-- ================================================================
SELECT
    CASE COLUMN_CATEGORY
        WHEN 1 THEN '1_PRIMARY_KEY'
        WHEN 2 THEN '2_FOREIGN_KEY'
        WHEN 3 THEN '3_OTHER_ID'
        WHEN 4 THEN '4_DATE_TIMESTAMP'
        WHEN 5 THEN '5_BOOLEAN'
        WHEN 6 THEN '6_TEXT'
        WHEN 7 THEN '7_NUMERIC'
        WHEN 8 THEN '8_ROW_ITERATION_DLT'
        ELSE '9_OTHER'
    END AS CATEGORY_NAME,
    COUNT(*) AS COLUMN_COUNT
FROM MS_RAW.STG_META.V_COLUMN_CLASSIFICATION
GROUP BY COLUMN_CATEGORY
ORDER BY COLUMN_CATEGORY;
