UPDATE MS_RAW.STG_META.SF_COLUMNS
   SET NEW_COLUMN_TYPE = CASE WHEN DATA_TYPE = 'TIMESTAMP_TZ' THEN 'TIMESTAMP_NTZ' END,
       BASE_COLUMN_NAME =
       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            '^_'||SF_COLUMN_NAME||'_$','_DATE_','_')
            ,'_RANGE_$','')
            ,'_ON_$','')
            ,'_AT_$','')
            ,'_TO_$','_UNTIL')
            ,'^_','')
            ,'_$','')
            ,'OF_BIRTH','BIRTH')
WHERE DATA_TYPE IN ('DATE','TIMESTAMP_TZ')
;
SELECT BASE_COLUMN_NAME, SF_COLUMN_NAME, SF_TABLE_NAME
FROM MS_RAW.STG_META.SF_COLUMNS
WHERE BASE_COLUMN_NAME IS NOT NULL
ORDER BY BASE_COLUMN_NAME;

CREATE OR REPLACE TABLE MS_RAW.STG_META.timestamp_column_analysis AS
SELECT * REPLACE( REPLACE(table_schema,'MS_','REEDONLINE_') AS table_schema)
FROM MS_RAW.DBT_META.timestamp_column_analysis
;
SELECT * FROM MS_RAW.STG_META.timestamp_column_analysis
ORDER BY COLUMN_NAME
--WHERE IS_DATE_ONLY
;
UPDATE MS_RAW.STG_META.SF_COLUMNS
   SET NEW_COLUMN_TYPE = 'DATE'
WHERE (SF_SCHEMA_NAME, SF_TABLE_NAME, SF_COLUMN_NAME) IN (SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME 
                                                          FROM MS_RAW.STG_META.timestamp_column_analysis
                                                          WHERE IS_DATE_ONLY)
   OR SF_COLUMN_NAME = 'DATE_OF_BIRTH'
;
UPDATE MS_RAW.STG_META.SF_COLUMNS
   SET NEW_COLUMN_NAME = BASE_COLUMN_NAME || '_DATE'
WHERE DATA_TYPE = 'DATE' 
   OR NEW_COLUMN_TYPE = 'DATE'
;
UPDATE MS_RAW.STG_META.SF_COLUMNS
   SET NEW_COLUMN_NAME = CASE WHEN ENDSWITH(BASE_COLUMN_NAME,'_FROM') 
                                OR ENDSWITH(BASE_COLUMN_NAME,'_UNTIL')
                              THEN BASE_COLUMN_NAME
                              ELSE BASE_COLUMN_NAME || '_AT'
                         END
WHERE NEW_COLUMN_TYPE = 'TIMESTAMP_NTZ'
;
INSERT INTO MS_RAW.STG_META.SF_COLUMNS
      (SF_SCHEMA_NAME, SF_TABLE_NAME, SF_COLUMN_NAME, BASE_COLUMN_NAME, NEW_COLUMN_NAME,             NEW_COLUMN_TYPE, DATA_TYPE)  
SELECT SF_SCHEMA_NAME, SF_TABLE_NAME, SF_COLUMN_NAME, BASE_COLUMN_NAME, BASE_COLUMN_NAME || '_DATE', 'DATE',          DATA_TYPE  
FROM MS_RAW.STG_META.SF_COLUMNS
WHERE DATA_TYPE = 'TIMESTAMP_TZ'
  AND NEW_COLUMN_TYPE = 'TIMESTAMP_NTZ'
  AND (SF_SCHEMA_NAME, SF_TABLE_NAME, SF_COLUMN_NAME) NOT IN (SELECT SF_SCHEMA_NAME, SF_TABLE_NAME, SF_COLUMN_NAME
                                                              FROM MS_RAW.STG_META.SF_COLUMNS
                                                             WHERE DATA_TYPE = 'TIMESTAMP_TZ'
                                                               AND NEW_COLUMN_TYPE = 'DATE')
ORDER BY BASE_COLUMN_NAME
;





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
        EXECUTE IMMEDIATE :query;
        full_query := :full_query || :query;
    END FOR;
    
    RETURN :full_query;
END;

SELECT * FROM MS_RAW.DBT_META.timestamp_column_analysis
;
*/

SELECT * FROM MS_RAW.DBT_META.timestamp_column_analysis
;