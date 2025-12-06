# Modelling MSSQL data ingested in Snowflake

You are an Analytics Engineering Consultant  that will continue the work related to this project (creatinng dbt staging and intermediate models following best practices). For security reason you are provided with a Snowflake db MS_RAW that has all the needed empty tables + snowflake version of the mssql information_schema views. If you need the real data (ROL_RAW) you have to provide SQL statements and will be given the results.

## 01 MS_RAW db Setup

Creating a new db MS_RAW that will contain all the needed info from ROL_RAW

Script:
- **01_Initial_Setup.sql**<br>Creates db MS_RAW and the same data SCHEMAs (excluding the prefix "REEDONLINE_" in the names) from ROL_RAW. This way the names of the schemas in MSSQL and MS_RAW are the same
- **02_ms_tables_like.sql**<br>Creates empty versions of the original tables
- **03_ms_meta.sql**<br>Copies the content of the MSSQL INFORMATION_SCHEMA views available in Snowflake

### Data

MS_RAW db contains the ingested from MSSQL tables in the following schemata:

MS_RAW.DBO;
MS_RAW.DUPLICATEJOBSERVICE;
MS_RAW.JOBIMPORT;
MS_RAW.JOBS;
MS_RAW.RECRUITERJOBSTEXTKERNEL;

### MSSQL Metadata

MS_RAW.MS_META contains:
- MSSQL full copies of MSDB.INFORMATION_SCHEMA views:
  - COLUMNS
  - KEY_COLUMN_USAGE
  - REFERENTIAL_CONSTRAINTS
  - TABLES
  - TABLE_CONSTRAINTS

## 02 Joining Snowflake with MSSQL metadata

Prepares combined Snowflake and MSSQL metadata for TABLES, COLUMNS

Scripts:
- 01_sf_tabless.sql<br>Joins the Snowflake TABLES metadata with MSSQL TABLES metadata based on the TABLE_KEY in SF_TABLES
- 02_sf_columns.sql<br>Joins the Snowflake COLUMNS metadata with MSSQL COLUMNS metadata based on the COLUMN_KEY in SF_COLUMNS
?? In SF_COLUMNS to eliminate the not needed columns ??


### 

