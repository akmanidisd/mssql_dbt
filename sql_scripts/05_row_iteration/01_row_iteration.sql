USE ROLE ACCOUNTADMIN;
USE SCHEMA MS_RAW.DBT_META;


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

