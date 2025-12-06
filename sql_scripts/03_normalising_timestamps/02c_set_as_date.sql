SELECT 
    table_catalog,
    table_schema,
    table_name,
    column_name,
    NEW_column_name,
    timestamp_column_analysis.column_key,
    total_rows,
    date_only_count,
    has_time_count,
    is_date_only,
    ROUND((date_only_count / NULLIF(total_rows, 0)) * 100, 2) as date_only_percentage
FROM  MS_RAW.DBT_META.timestamp_column_analysis 
INNER JOIN  MS_RAW.DBT_META.NEW_DATETIME_COLUMNS
   ON timestamp_column_analysis.column_key = NEW_DATETIME_COLUMNS.column_key
where is_date_only
ORDER BY table_catalog, table_schema, table_name, column_name
;

UPDATE MS_RAW.DBT_META.NEW_DATETIME_COLUMNS
   SET NEW_DATA_TYPE = 'DATE'
WHERE NEW_DATA_TYPE IS NULL
  AND column_key IN (SELECT column_key FROM  MS_RAW.DBT_META.timestamp_column_analysis WHERE is_date_only)
;

UPDATE MS_RAW.DBT_META.NEW_DATETIME_COLUMNS
   SET NEW_DATA_TYPE = 'TIMESTAMP_NTZ'
WHERE NEW_DATA_TYPE IS NULL
  AND SF_DATA_TYPE = 'TIMESTAMP_TZ'
;

SELECT timestamp_column_analysis.*, NEW_DATETIME_COLUMNS.* 
  FROM MS_RAW.DBT_META.timestamp_column_analysis
     , MS_RAW.DBT_META.NEW_DATETIME_COLUMNS 
 WHERE is_date_only
   AND timestamp_column_analysis.column_key = NEW_DATETIME_COLUMNS.column_key
ORDER BY ALL
;
