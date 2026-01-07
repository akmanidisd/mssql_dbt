Using 

MS_RAW.STG_META.SF_COLUMNS sfc
INNER JOIN MS_RAW.STG_META.SF_TABLES sft
  ON sfc.SF_TABLE_SCHEMA = sft.SF_TABLE_SCHEMA
 AND sfc.SF_TABLE_NAME = sft.SF_TABLE_NAME

create scripts for Snowflake views and write the in  

/scripts/staging_snowflake/<sft.SF_TABLE_SCHEMA>/STG_<sft.NEW_TABLE_NAME>

```sql
-- pseudo SQL
CREATE OR REPLACE VIEW MS_RAW.<sft.SF_TABLE_SCHEMA>.STG_<sft.NEW_TABLE_NAME> AS
SELECT
-- PK FROM sft.SF_TABLE_NAME
-- FK FROM T_SECONDARY_KEYS
-- OTHER COLUMNS WITH NAMES ENDING WITH "_id"
-- DATE/TIMESTAMP COLUMNS ALPHABETICALLY
-- BOOLEAN
-- TEXT
-- NUMERIC
-- _ROW_ITERATION & _DLT
FROM MS_RAW.<sft.SF_TABLE_SCHEMA><sft.SF_TABLE_NAME>

Column Namings

(<sfc.SF_COLUMN_NAME>|(<sfc.NEW_COLUMN_EXPRESSION>))[<sfc.NEW_COLUMN_TYPE>] [as <sfc.NEW_COLUMN_TYPE>
```  

Created Files:
scripts/staging_meta/19_generate_view_scripts.sql - Main SQL script that:
Creates V_COLUMN_CLASSIFICATION view to classify columns into 8 categories (PK, FK, other IDs, dates/timestamps, booleans, text, numeric, row iteration/DLT)
Creates V_GENERATED_VIEW_SCRIPTS view that generates complete DDL scripts for each staging view
Includes validation queries and statistics
scripts/staging_meta/20_export_view_scripts.sql - Export helper script with multiple options:
Export by individual schema
Create combined deployment scripts per schema
Master deployment script for all schemas
Validation and testing queries
scripts/generate_view_files.py - Python automation script that:
Connects to Snowflake using environment variables
Fetches generated view scripts from metadata
Writes individual view files to scripts/staging_snowflake/<SCHEMA>/STG_<TABLE_NAME>.sql
Creates combined _DEPLOY_ALL.sql per schema
Provides detailed statistics
requirements.txt - Python dependencies file with snowflake-connector-python
Usage Instructions:
Step 1: Execute SQL scripts in Snowflake

-- Run in order:
-- 19_generate_view_scripts.sql (creates the generator views)
-- 20_export_view_scripts.sql (optional, for manual review)
Step 2: Run Python script to create files

# Install dependencies
pip install -r requirements.txt

# Set Snowflake credentials
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=your_warehouse  # optional

# Generate view files
python scripts/generate_view_files.py
This will create files like:
scripts/staging_snowflake/REEDONLINE_DBO/STG_USER.sql
scripts/staging_snowflake/REEDONLINE_DBO/STG_JOB.sql
scripts/staging_snowflake/REEDONLINE_DBO/_DEPLOY_ALL.sql
The views will have columns ordered exactly as specified in the README: PK → FK → Other IDs → Date/Timestamp → Boolean → Text → Numeric → Row Iteration/DLT.
