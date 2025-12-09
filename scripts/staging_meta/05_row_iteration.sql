INSERT INTO MS_RAW.STG_META.SF_COLUMNS
      (SF_SCHEMA_NAME, SF_TABLE_NAME, SF_COLUMN_NAME, 
       BASE_COLUMN_NAME, NEW_COLUMN_NAME, NEW_COLUMN_TYPE, DATA_TYPE) 
SELECT SF_SCHEMA_NAME, SF_TABLE_NAME, SF_COLUMN_NAME, 
       CASE WHEN SF_COLUMN_NAME = 'TIME_STAMP' 
            THEN 'ROW_ITERATION'
            ELSE SF_COLUMN_NAME
       END AS NEW_BASE_COLUMN_NAME,
       NEW_BASE_COLUMN_NAME || '_NUM' AS NEW_COLUMN_NAME, 
       'TO_NUMBER(TO_VARCHAR({COLUMN_NAME}), ''XXXXXXXXXXXXXXXX'')' AS NEW_COLUMN_TYPE,
       DATA_TYPE
FROM MS_RAW.STG_META.SF_COLUMNS
WHERE DATA_TYPE = 'BINARY'
  AND SF_COLUMN_NAME ILIKE ANY ('%ROW%ITERATION%','TIME_STAMP')
  AND (SF_SCHEMA_NAME, SF_TABLE_NAME, SF_COLUMN_NAME) NOT IN (SELECT SF_SCHEMA_NAME, SF_TABLE_NAME, SF_COLUMN_NAME
                                                              FROM MS_RAW.STG_META.SF_COLUMNS
                                                              WHERE NEW_COLUMN_TYPE = 'TO_NUMBER(TO_VARCHAR({COLUMN_NAME}), ''XXXXXXXXXXXXXXXX'')' )
ORDER BY ALL
;

SELECT *
FROM MS_RAW.STG_META.SF_COLUMNS
WHERE DATA_TYPE = 'BINARY'
  AND SF_COLUMN_NAME ILIKE ANY ('%ROW%ITERATION%','TIME_STAMP')
--  AND NEW_COLUMN_TYPE = 'TO_NUMBER(TO_VARCHAR({COLUMN_NAME}), ''XXXXXXXXXXXXXXXX'')'
ORDER BY ALL
;


-- for jobs -> time_stamp -> row_iteration_num
select
    to_number(to_varchar(time_stamp), 'XXXXXXXXXXXXXXXX') as row_iteration_num,  --BEST
--from rol_raw.reedonline_dbo.jobs
from rol_raw.reedonline_dbo_frequent.jobs
limit 1;

-- for job_search_alert -> sjs_row_iteration -> sjs_row_iteration_num
select
    to_number(to_varchar(sjs_row_iteration), 'XXXXXXXXXXXXXXXX') as sjs_row_iteration_num,  --BEST
--from rol_raw.reedonline_dbo.job_search_alert
from rol_raw.reedonline_dbo_frequent.job_search_alert
limit 1;

-- for ALL the rest -> row_iteration -> row_iteration_num
select
    to_number(to_varchar(row_iteration), 'XXXXXXXXXXXXXXXX') as row_iteration_num,  --BEST
from rol_raw.reedonline_dbo.users
limit 1;