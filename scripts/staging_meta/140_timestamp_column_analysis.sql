-- run only once !!!!
/*
CREATE OR REPLACE TABLE MS_RAW.DBT_META.timestamp_column_analysis (
    table_catalog VARCHAR,
    table_schema VARCHAR,
    table_name VARCHAR,
    column_name VARCHAR,
    column_key VARCHAR,
    total_rows NUMBER,
    date_only_count NUMBER,
    has_time_count NUMBER,
    is_date_only BOOLEAN,
    analyzed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- UNCOMMENT and RUN the script
DECLARE
    catalog VARCHAR;
    schema_name VARCHAR;
    table_name VARCHAR;
    column_name VARCHAR;
    column_key VARCHAR;
    query VARCHAR;
    full_query VARCHAR := '';
    cur CURSOR FOR 
        SELECT 
            SF_TABLE_CATALOG,
            SF_TABLE_SCHEMA,
            SF_TABLE_NAME,
            SF_COLUMN_NAME,
            COLUMN_KEY
        FROM MS_RAW.DBT_META.NEW_DATETIME_COLUMNS
        WHERE SF_DATA_TYPE != 'DATE'
        ORDER BY ALL;
BEGIN
    -- Loop through each timestamp column
    FOR record IN cur DO
        catalog := record.SF_TABLE_CATALOG;
        schema_name := record.SF_TABLE_SCHEMA;
        table_name := record.SF_TABLE_NAME;
        column_name := record.SF_COLUMN_NAME;        
        column_key := record.COLUMN_KEY;        
        -- Build dynamic SQL to analyze the column
        query := '
            INSERT INTO MS_RAW.DBT_META.timestamp_column_analysis 
                (table_catalog, table_schema, table_name, column_name, column_key,
                 total_rows, date_only_count, has_time_count, is_date_only)
            SELECT 
                ''' || catalog || ''',
                ''' || schema_name || ''',
                ''' || table_name || ''',
                ''' || column_name || ''',
                ''' || column_key || ''',
                COUNT(*) as total_rows,
                COUNT_IF(' || column_name || ' = DATE_TRUNC(''DAY'', ' || column_name || ')) as date_only_count,
                COUNT_IF(' || column_name || ' != DATE_TRUNC(''DAY'', ' || column_name || ')) as has_time_count,
                COUNT_IF(' || column_name || ' != DATE_TRUNC(''DAY'', ' || column_name || ')) = 0 as is_date_only
            FROM ROL_RAW.REEDONLINE_' || REPLACE(schema_name,'MS_','') || '.' || table_name || '
            WHERE ' || column_name || ' IS NOT NULL
            ;\n';
        
        -- Execute the query
        --EXECUTE IMMEDIATE :query;
        full_query := :full_query || :query;
    END FOR;
    
    RETURN :full_query;
END;

*/

SELECT * FROM MS_RAW.DBT_META.timestamp_column_analysis
;
