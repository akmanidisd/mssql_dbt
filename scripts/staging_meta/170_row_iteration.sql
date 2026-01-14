-- NEW_COLUMN_NAME 
UPDATE MS_RAW.STG_META.SF_COLUMNS
   SET NEW_COLUMN_NAME = CASE WHEN COLUMN_NAME  = 'TIME_STAMP'
                              THEN '_ROW_ITERATION'
                              ELSE '_' || COLUMN_NAME
                         END
WHERE COLUMN_NAME ILIKE ANY ('%ROW_ITERATION','TIME_STAMP')
;


INSERT INTO MS_RAW.STG_META.SF_COLUMNS
      (TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, 
       NEW_COLUMN_NAME, NEW_COLUMN_EXPRESSION, DATA_TYPE) 
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, 
       NEW_COLUMN_NAME || '_NUM' AS NEW_COLUMN_NAME, 
       'TO_NUMBER(TO_VARCHAR(' || COLUMN_NAME || '), ''XXXXXXXXXXXXXXXX'')' AS NEW_COLUMN_EXPRESSION,
       DATA_TYPE
FROM MS_RAW.STG_META.SF_COLUMNS
WHERE DATA_TYPE = 'BINARY'
  AND NEW_COLUMN_NAME ILIKE '%ROW_ITERATION'
  AND (TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME) NOT IN (SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, 
                                                      FROM MS_RAW.STG_META.SF_COLUMNS
                                                      WHERE NEW_COLUMN_NAME ILIKE '%ROW_ITERATION_NUM' )
ORDER BY ALL
;


SELECT NEW_COLUMN_NAME, * EXCLUDE (NEW_COLUMN_NAME)
FROM MS_RAW.STG_META.SF_COLUMNS
WHERE DATA_TYPE = 'BINARY'
  AND COLUMN_NAME ILIKE ANY ('%ROW%ITERATION%','TIME_STAMP')
  AND COLUMN_NAME != 'ROW_ITERATION'
--  AND NEW_COLUMN_TYPE = 'TO_NUMBER(TO_VARCHAR({COLUMN_NAME}), ''XXXXXXXXXXXXXXXX'')'
ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, NEW_COLUMN_NAME
;

/*
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
*/

-- for ALL the rest -> row_iteration -> row_iteration_num
select
    to_number(to_varchar(row_iteration), 'XXXXXXXXXXXXXXXX') as row_iteration_num,  --BEST
from rol_raw.reedonline_dbo.users
limit 1;