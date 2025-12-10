UPDATE MS_RAW.STG_META.SF_COLUMNS
   SET NEW_COLUMN_TYPE = CASE WHEN DATA_TYPE = 'TIMESTAMP_TZ' THEN 'TIMESTAMP_NTZ' END,
       BASE_COLUMN_NAME =
       REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
            '^_'||SF_COLUMN_NAME||'_$'
            ,'_DATE_','_')
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
ORDER BY BASE_COLUMN_NAME
;

-- UNCOMMENT and RUN the script AT THE END (RUN ONLY ONCE!!! VERY HAVY)
SELECT * FROM MS_RAW.STG_META.timestamp_column_analysis
WHERE IS_DATE_ONLY
ORDER BY SF_COLUMN_NAME
;

UPDATE MS_RAW.STG_META.SF_COLUMNS
   SET NEW_COLUMN_TYPE = 'DATE'
WHERE (SF_TABLE_SCHEMA, SF_TABLE_NAME, SF_COLUMN_NAME) IN (SELECT SF_TABLE_SCHEMA, SF_TABLE_NAME, SF_COLUMN_NAME
                                             FROM MS_RAW.STG_META.timestamp_column_analysis
                                            WHERE IS_DATE_ONLY)
   OR SF_COLUMN_NAME = 'DATE_OF_BIRTH'
;

UPDATE MS_RAW.STG_META.SF_COLUMNS
   SET NEW_COLUMN = BASE_COLUMN || '_DATE'
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
      (SF_TABLE_SCHEMA, SF_TABLE_NAME, SF_COLUMN_NAME, BASE_COLUMN_NAME, NEW_COLUMN_NAME,             NEW_COLUMN_TYPE, DATA_TYPE)  
SELECT SF_TABLE_SCHEMA, SF_TABLE_NAME, SF_COLUMN_NAME, BASE_COLUMN_NAME, BASE_COLUMN_NAME || '_DATE', 'DATE',          DATA_TYPE  
FROM MS_RAW.STG_META.SF_COLUMNS
WHERE DATA_TYPE = 'TIMESTAMP_TZ'
  AND NEW_COLUMN_TYPE = 'TIMESTAMP_NTZ'
  AND (SF_TABLE_SCHEMA, SF_TABLE_NAME, SF_COLUMN_NAME) NOT IN (SELECT SF_TABLE_SCHEMA, SF_TABLE_NAME, SF_COLUMN_NAME
                                                                FROM MS_RAW.STG_META.SF_COLUMNS
                                                               WHERE DATA_TYPE = 'TIMESTAMP_TZ'
                                                                 AND NEW_COLUMN_TYPE = 'DATE')
ORDER BY BASE_COLUMN_NAME
;







/*
-- USE WH SEARCH_MEDIUM_WH
-- TABLE MS_RAW.STG_META.timestamp_column_analysis
-- UNCOMMENT and RUN the script
DECLARE
    sf_table_schema VARCHAR;
    sf_table_name VARCHAR;
    sf_column_name VARCHAR;
    one_query VARCHAR;
    full_query VARCHAR := '';
    cur CURSOR FOR 
        SELECT 
            SF_TABLE_SCHEMA,
            SF_TABLE_NAME,
            SF_COLUMN_NAME
        FROM MS_RAW.STG_META.SF_COLUMNS
        WHERE DATA_TYPE = 'TIMESTAMP_TZ'
          AND SF_COLUMN_NAME NOT IN ('CREATED_ON','UPDATED_ON')
        ORDER BY ALL;
BEGIN

    CREATE OR REPLACE TABLE MS_RAW.STG_META.timestamp_column_analysis (
        sf_table_schema VARCHAR,
        sf_table_name VARCHAR,
        sf_column_name VARCHAR,
        total_rows NUMBER,
        date_only_count NUMBER,
        has_time_count NUMBER,
        is_date_only BOOLEAN,
        analyzed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    );
    -- Loop through each timestamp column
    FOR record IN cur DO
        sf_table_schema := record.SF_TABLE_SCHEMA;
        sf_table_name   := record.SF_TABLE_NAME;
        sf_column_name  := record.SF_COLUMN_NAME;        
       
        -- Build dynamic SQL to analyze the column
        one_query := '
            INSERT INTO MS_RAW.STG_META.timestamp_column_analysis 
                (sf_table_schema, sf_table_name, sf_column_name, 
                 total_rows, date_only_count, has_time_count, is_date_only)
            SELECT 
                ''' || sf_table_schema || ''',
                ''' || sf_table_name || ''',
                ''' || sf_column_name || ''',
                COUNT(*) as total_rows,
                COUNT_IF(' || sf_column_name || '  = DATE_TRUNC(''DAY'', ' || sf_column_name || ')) as date_only_count,
                COUNT_IF(' || sf_column_name || ' != DATE_TRUNC(''DAY'', ' || sf_column_name || ')) as has_time_count,
                COUNT_IF(' || sf_column_name || ' != DATE_TRUNC(''DAY'', ' || sf_column_name || ')) = 0 as is_date_only
            FROM ROL_RAW.' || sf_table_schema || '.' || sf_table_name || '
            WHERE ' || sf_column_name || ' IS NOT NULL
            ;\n';
        
        -- Execute the query
        EXECUTE IMMEDIATE :one_query;
        full_query := :full_query || :one_query;
    END FOR;
    
    RETURN :full_query;
END;

SELECT * FROM MS_RAW.STG_META.timestamp_column_analysis WHERE IS_DATE_ONLY
;
*/