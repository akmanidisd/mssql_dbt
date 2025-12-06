USE ROLE ACCOUNTADMIN;
USE SCHEMA MS_RAW.DBT_META;


select _dlt_load_id::NUMBER(18,7)       as _dlt_load_id_num, 
       _dlt_load_id_num::timestamp_ntz(9) as _dlt_loaded_at
from rol_raw.reedonline_dbo.users
limit 1;



