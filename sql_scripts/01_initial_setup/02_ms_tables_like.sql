USE ROLE ACCOUNTADMIN;


-- The statement creates the script for all the tables
-- CREATE TABLE IF NOT EXISTS MS_RAW.<schema>.<table>
--     LIKE ROL_RAW.REEDONLINE_<schema>.<table>;
WITH ms_schemata AS (
    SELECT SCHEMA_NAME AS ROL_SCHEMA,
           REPLACE(REPLACE(SCHEMA_NAME,'REEDONLINE_',''),'RESTRICTED','DBO') AS MS_SCHEMA
    FROM ROL_RAW.INFORMATION_SCHEMA.SCHEMATA 
    WHERE SCHEMA_NAME IN ('REEDONLINE_DBO'
                         ,'REEDONLINE_DUPLICATEJOBSERVICE'
                         ,'REEDONLINE_JOBIMPORT'
                         ,'REEDONLINE_JOBS'
                         ,'REEDONLINE_RECRUITERJOBSTEXTKERNEL'
                         ,'REEDONLINE_RESTRICTED')
)
, raw_tables as (
    SELECT MS_SCHEMA, ROL_SCHEMA, TABLE_NAME
    FROM ROL_RAW.INFORMATION_SCHEMA.TABLES
    INNER JOIN ms_schemata
        ON TABLE_SCHEMA = ROL_SCHEMA
    WHERE NOT TABLE_NAME LIKE ANY ('_DLT%')
)
select listagg(
    'CREATE TABLE IF NOT EXISTS'
    || ' MS_RAW.'    || MS_SCHEMA || '.' || TABLE_NAME
    || '\n             LIKE'
    || ' ROL_RAW.' || ROL_SCHEMA || '.' || TABLE_NAME
    || ';\n'
    ) WITHIN GROUP (ORDER BY MS_SCHEMA, TABLE_NAME) AS  "--STMT"
from  raw_tables
ORDER BY 1
;

