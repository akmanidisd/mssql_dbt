-- run only once !!!!
/*
CREATE OR REPLACE TABLE MS_RAW.STG_META.TIMESTAMP_COLUMN_ANALYSIS (
    SF_TABLE_SCHEMA VARCHAR,
    SF_TABLE_NAME VARCHAR,
    SF_COLUMN_NAME VARCHAR,
    TOTAL_ROWS NUMBER,
    DATE_ONLY_COUNT NUMBER,
    HAS_TIME_COUNT NUMBER,
    IS_DATE_ONLY BOOLEAN,
    ANALYZED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- UNCOMMENT and RUN the script
DECLARE
    schema_name VARCHAR;
    table_name VARCHAR;
    column_name VARCHAR;
    query VARCHAR;
    full_query VARCHAR := '';
    cur CURSOR FOR 
        SELECT 
            SF_TABLE_SCHEMA,
            SF_TABLE_NAME,
            SF_COLUMN_NAME,
        FROM MS_RAW.STG_META.SF_COLUMNS
        WHERE DATA_TYPE ILIKE 'TIMESTAMP_%'
        ORDER BY ALL;
BEGIN
    -- Loop through each timestamp column
    FOR record IN cur DO
        schema_name := record.SF_TABLE_SCHEMA;
        table_name := record.SF_TABLE_NAME;
        column_name := record.SF_COLUMN_NAME;        

        -- Build dynamic SQL to analyze the column
        query := '
            INSERT INTO MS_RAW.STG_META.TIMESTAMP_COLUMN_ANALYSIS 
                (SF_TABLE_SCHEMA, SF_TABLE_NAME, SF_COLUMN_NAME,
                 TOTAL_ROWS, DATE_ONLY_COUNT, HAS_TIME_COUNT, IS_DATE_ONLY)
            SELECT 
                ''' || schema_name || ''',
                ''' || table_name || ''',
                ''' || column_name || ''',
                COUNT(*) as total_rows,
                COUNT_IF(' || column_name || ' = DATE_TRUNC(''DAY'', ' || column_name || ')) as date_only_count,
                COUNT_IF(' || column_name || ' != DATE_TRUNC(''DAY'', ' || column_name || ')) as has_time_count,
                COUNT_IF(' || column_name || ' != DATE_TRUNC(''DAY'', ' || column_name || ')) = 0 as is_date_only
            FROM ROL_RAW.' || schema_name || '.' || table_name || '
            WHERE ' || column_name || ' IS NOT NULL
            ;\n';
        
        -- Execute the query
        -- EXECUTE IMMEDIATE :query;
        full_query := :full_query || :query;
    END FOR;
    
    RETURN :full_query;
END;

*/
;

SELECT * FROM MS_RAW.STG_META.TIMESTAMP_COLUMN_ANALYSIS
WHERE IS_DATE_ONLY
ORDER BY ALL
;

SELECT * FROM MS_RAW.STG_META.TIMESTAMP_COLUMN_ANALYSIS
WHERE SF_COLUMN_NAME LIKE '%BIRTH%'
ORDER BY ALL
;

