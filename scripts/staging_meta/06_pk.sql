    CREATE OR REPLACE VIEW MS_RAW.STG_META.V_PRIMARY_KEYS AS
    SELECT
        sfc.SF_TABLE_SCHEMA,
        sfc.SF_TABLE_NAME,
        sfc.SF_COLUMN_NAME,
        sfc.MS_TABLE_SCHEMA,
        sfc.MS_TABLE_NAME,
        sfc.MS_COLUMN_NAME,
        sfc.NEW_COLUMN_NAME,
        sft.NEW_TABLE_NAME,
        kcu.CONSTRAINT_NAME,
        COUNT(*) OVER (PARTITION BY kcu.CONSTRAINT_NAME) AS PK_COLUMNS,
        COUNT(DISTINCT kcu.CONSTRAINT_NAME) OVER (PARTITION BY sfc.SF_COLUMN_NAME) AS PK_TABLES,
        kcu.ORDINAL_POSITION
    FROM ROL_RAW.REEDONLINE_META.TABLE_CONSTRAINTS tc
    INNER JOIN ROL_RAW.REEDONLINE_META.KEY_COLUMN_USAGE kcu
        ON  kcu.TABLE_SCHEMA    = tc.TABLE_SCHEMA
        AND kcu.TABLE_NAME      = tc.TABLE_NAME
        AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    INNER JOIN MS_RAW.STG_META.SF_COLUMNS sfc
        ON  kcu.TABLE_SCHEMA = sfc.MS_TABLE_SCHEMA
        AND kcu.TABLE_NAME   = sfc.MS_TABLE_NAME
        AND kcu.COLUMN_NAME  = sfc.MS_COLUMN_NAME
    INNER JOIN MS_RAW.STG_META.SF_TABLES sft
        ON sfc.SF_TABLE_SCHEMA = sft.SF_TABLE_SCHEMA
       AND sfc.SF_TABLE_NAME   = sft.SF_TABLE_NAME
    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
      AND sft.SF_TABLE_NAME != 'PRICEBOOK_ENTRY' -- pk has 5 columns
    ORDER BY ALL;

    -- 298 PKEYS, 320 PK Columns
    SELECT count(distinct CONSTRAINT_NAME) as TOTAL_PKEYS
         , count(*) AS TOTAL_COLUMNS
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS;

    -- 220 (PK_COLUMNS=1 & PK_TABLES=1)
    SELECT count(distinct CONSTRAINT_NAME) as TOTAL_PKEYS
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS
    WHERE PK_COLUMNS = 1
      AND PK_TABLES = 1;
    
    -- 56 (PK_COLUMNS=1 & PK_TABLES>1)
    SELECT count(distinct CONSTRAINT_NAME) as TOTAL_PKEYS
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS
    WHERE PK_COLUMNS = 1
      AND PK_TABLES > 1;
    
  
    -- 220 pk has one column AND exists in one table
    CREATE OR REPLACE VIEW MS_RAW.STG_META.V_PRIMARY_KEYS_ONE AS
    SELECT SF_TABLE_SCHEMA, 
           SF_TABLE_NAME, 
           NEW_TABLE_NAME, 
           SF_COLUMN_NAME, 
           NEW_TABLE_NAME || '_ID' AS NEW_PK_COLUMN_NAME,
           COUNT(*) OVER (PARTITION BY SF_TABLE_SCHEMA, SF_COLUMN_NAME) AS PK_TABLES,
           SF_COLUMN_NAME = NEW_PK_COLUMN_NAME AS IS_TABLE_ID
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS K
    WHERE PK_COLUMNS = 1
      AND PK_TABLES = 1
    ORDER BY SF_TABLE_SCHEMA, NEW_TABLE_NAME; 

    -- 220
    SELECT *
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS_ONE;
    
    -- 191 pk = <table_name>_id
    SELECT *
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS_ONE K
    WHERE SF_COLUMN_NAME = NEW_PK_COLUMN_NAME
    ;

    -- 191
    UPDATE MS_RAW.STG_META.SF_TABLES AS t
       SET PRIMARY_KEY_NAME = i.sf_column_name,
           PK_COLUMNS  = 1,
           PK_TABLES  = 1,
           NEW_PRIMARY_KEY_NAME = i.sf_column_name
      FROM MS_RAW.STG_META.V_PRIMARY_KEYS_ONE AS i
      WHERE t.sf_table_schema = i.sf_table_schema
        AND t.sf_table_name   = i.sf_table_name
        AND i.SF_COLUMN_NAME  = i.NEW_PK_COLUMN_NAME
    ;

    -- 29 pk != <table_name>_id
    SELECT *
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS_ONE K
    WHERE SF_COLUMN_NAME != NEW_PK_COLUMN_NAME
    ;

    -- INVESTIGATE WHICH PK TO BE RENAMED
    SELECT i.sf_column_name,
           i.sf_table_name = c.sf_table_name as IS_PK,
           i.NEW_PK_COLUMN_NAME,
           c.sf_table_NAME,
           i.NEW_TABLE_NAME,
           c.sf_table_schema
      FROM MS_RAW.STG_META.SF_COLUMNS AS c
      INNER JOIN MS_RAW.STG_META.V_PRIMARY_KEYS_ONE AS i
          ON c.sf_table_schema = i.sf_table_schema
             and c.sf_column_name = i.sf_column_name
      WHERE 
          i.SF_COLUMN_NAME != i.NEW_PK_COLUMN_NAME
    ORDER BY ALL;

    /*
             i.sf_column_name     IN ('CC_STATUS','CLIENT_ID','DISPLAY_STYLE_ID','DRAFT_ID',
                                      'DUPLICATE_JOBS_ID','ECRUITER_FEEDBACK_ID',
                                      'ECRUITER_FEEDBACK_TYPE_ID','EMAIL_ACTION_ID',
                                      'ENQUIRY_LOG_STATUS_ID','FEEDBACK_ID','GATEWAY_ID',
                                      'IMPORT_FEED_ECRUITER_ID','IN_ARREARS_JOB_SPENDS_ID',
                                      'JOB_SEARCH_ALERT_CHANNEL_LOOKUP_ID',
                                      'JOB_SECTOR_VISIBILITY_BW','JOB_SPEND_REASON_BW',
                                      'LEVEL_ID','LOG_TYPE','NOTICE_ID','ORG_ID',
                                      'OU_BETA_ACCESS_BW','PROFICIENCY_ID','PROFILE_ID',
                                      'PUSH_NOTIFICATION_PREFERENCE_BW','REG_TYPE_ID',
                                      'SALARY_DESCRIPTION_BW','SEARCH_TYPE',
                                      'SPENT_ON_TYPE_ID','TEMPLATE_ID','USER_JOB_HISTORY_ID'
                                     )
    */

    -- 29
    CREATE OR REPLACE VIEW MS_RAW.STG_META.V_PRIMARY_KEYS_ONE_OLD_NAME AS
    SELECT DISTINCT i.sf_column_name,
      FROM MS_RAW.STG_META.SF_COLUMNS AS c
      INNER JOIN MS_RAW.STG_META.V_PRIMARY_KEYS_ONE AS i
          ON c.sf_table_schema = i.sf_table_schema
             and c.sf_column_name = i.sf_column_name
      WHERE 
          i.SF_COLUMN_NAME != i.NEW_PK_COLUMN_NAME
    ORDER BY ALL;

    -- 29
    SELECT * FROM MS_RAW.STG_META.V_PRIMARY_KEYS_ONE_OLD_NAME;
    -- 
   
    -- NEW_PRIMARY_KEY_NAME = NEW_PK_COLUMN_NAME = <TABLE>_ID
    -- 29
    UPDATE MS_RAW.STG_META.SF_TABLES AS t
       SET PRIMARY_KEY_NAME = i.sf_column_name,
           PK_COLUMNS  = 1,
           PK_TABLES  = 1,
           NEW_PRIMARY_KEY_NAME = i.NEW_PK_COLUMN_NAME
      FROM MS_RAW.STG_META.V_PRIMARY_KEYS_ONE AS i
      WHERE t.sf_table_schema = i.sf_table_schema
        AND t.sf_table_name   = i.sf_table_name
        AND i.sf_column_name IN (SELECT sf_column_name 
                                 FROM MS_RAW.STG_META.V_PRIMARY_KEYS_ONE_OLD_NAME)
    ;

    -- LAST CHECK 191 , 29  = 220
    SELECT COUNT(CASE WHEN PRIMARY_KEY_NAME  = NEW_TABLE_NAME || '_ID' THEN 1 END) AS ID_NAMES,
           COUNT(CASE WHEN PRIMARY_KEY_NAME != NEW_TABLE_NAME || '_ID' THEN 1 END) AS OTHER_NAMES,
           COUNT(*)
    FROM MS_RAW.STG_META.SF_TABLES
    WHERE PK_COLUMNS = 1
      AND PK_TABLES  = 1;   

      

----------- END OF READY FOR  PK_COLUMNS = 1  AND PK_TABLES = 1

    -- 56 (PK_COLUMNS=1 & PK_TABLES>1)
    SELECT COALESCE(NEW_COLUMN_NAME,SF_COLUMN_NAME) AS COLUMN_NAME,
           CASE WHEN COLUMN_NAME = NEW_TABLE_NAME || '_ID' THEN 1 ELSE 0 END AS IS_MASTER,
           SUM(IS_MASTER) OVER (PARTITION BY SF_COLUMN_NAME) AS TOTAL_MASTERS,
           SF_COLUMN_NAME,
           NEW_COLUMN_NAME,
           NEW_TABLE_NAME,
           SF_TABLE_NAME
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS
    WHERE PK_COLUMNS = 1
      AND PK_TABLES  > 1
    QUALIFY TOTAL_MASTERS = 1
    ORDER BY SF_COLUMN_NAME, IS_MASTER DESC, SF_TABLE_NAME;


???????????????????????????????

---------- NOW 2 COLUMNS
    /*
      -- TO KEEP
         AND i.sf_column_name NOT IN ('API_TOKEN_ACCESS_LOG_ID','JOB_ID','OU_ID',
                                      'PRODUCT_BASE_ID','CANDIDATE_ID','ECRUITER_ID',
                                      'USER_ID','SALARY_TYPE_ID'
                                     )
    */


   -- CREATE OR REPLACE VIEW MS_RAW.STG_META.V_PRIMARY_KEYS_TWO AS
    SELECT pk.SF_TABLE_SCHEMA, 
           pk.SF_TABLE_NAME, 
           pk.NEW_TABLE_NAME, 
           pk.NEW_TABLE_NAME || '_ID' AS NEW_PK_COLUMN_NAME,
           pk.ORDINAL_POSITION,
           pk.SF_COLUMN_NAME,
           sfc.NEW_COLUMN_NAME
  --         MAX(CASE WHEN pk.ORDINAL_POSITION = 1 THEN sfc.NEW_COLUMN_NAME END) AS KEY_PART_1,
  --         MAX(CASE WHEN pk.ORDINAL_POSITION = 2 THEN sfc.NEW_COLUMN_NAME END) AS KEY_PART_2,
  --         KEY_PART_1 || ' * 10^12 + ' || KEY_PART_2                           AS NEW_PK_COLUMN_EXPRESSION
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS pk
    INNER JOIN MS_RAW.STG_META.SF_COLUMNS sfc
        ON  pk.SF_TABLE_SCHEMA = sfc.SF_TABLE_SCHEMA
        AND pk.SF_TABLE_NAME   = sfc.SF_TABLE_NAME
        AND pk.SF_COLUMN_NAME  = sfc.SF_COLUMN_NAME
    WHERE pk.PK_COLUMNS = 2
      AND pk.SF_TABLE_NAME != 'PRICEBOOK_ENTRY'
    --GROUP BY ALL
    --  AND pk.ORDINAL_POSITION > 2
    ORDER BY ALL
    ;

    SELECT * FROM MS_RAW.STG_META.V_PRIMARY_KEYS 
     WHERE PK_COLUMNS = 2
       AND NEW_COLUMN_NAME IS NULL
    ORDER BY ALL;

    SELECT * FROM MS_RAW.STG_META.V_PRIMARY_KEYS WHERE SF_COLUMN_NAME='CANDIDATE_ID' AND PK_COLUMNS = 1;
    SELECT * FROM MS_RAW.STG_META.SF_TABLES WHERE PRIMARY_KEY_NAME='CANDIDATE_ID';

    CREATE TABLE MS_RAW.STG_META.NEW_COLUMN_NAMES (
        SF_TABLE_SCHEMA VARCHAR,
        SF_TABLE_NAME   VARCHAR,
        SF_COLUMN_NAME  VARCHAR,
        NEW_COLUMN_NAME VARCHAR
    );
    INSERT INTO MS_RAW.STG_META.NEW_COLUMN_NAMES
    VALUES (),
           ()
    ;
        
/*
    UPDATE MS_RAW.STG_META.SF_COLUMNS AS c
       SET NEW_COLUMN_NAME = t.NEW_PRIMARY_KEY_NAME
    WHERE (SF_TABLE_SCHEMA, SF_TABLE_NAME, SF_COLUMN_NAME) IN (
       SELECT NULL::STRING SF_TABLE_SCHEMA, NULL::STRING SF_TABLE_SCHEMA, NULL::STRING SF_COLUMN_NAME, NULL::STRING SF_COLUMN_NAME
       UNION ALL
       SELECT 'X','V','X'
    )
    ;
*/
    CREATE OR REPLACE VIEW MS_RAW.STG_META.V_PRIMARY_KEYS_TWO AS
    SELECT SF_TABLE_SCHEMA, 
           SF_TABLE_NAME, 
           NEW_TABLE_NAME, 
           NEW_TABLE_NAME || '_ID' AS NEW_PK_COLUMN_NAME,
           ORDINAL_POSITION,
           SF_COLUMN_NAME,
  --         MAX(CASE WHEN ORDINAL_POSITION = 1 THEN SF_COLUMN_NAME END) AS KEY_PART_1,
  --         MAX(CASE WHEN ORDINAL_POSITION = 2 THEN SF_COLUMN_NAME END) AS KEY_PART_2,
  --         (KEY_PART_1 || ' * 10^12 + ' || KEY_PART_2)                 AS NEW_PK_COLUMN_EXPRESSION
    FROM MS_RAW.STG_META.V_PRIMARY_KEYS
    WHERE PK_COLUMNS = 2
      AND SF_TABLE_NAME != 'PRICEBOOK_ENTRY'
    GROUP BY ALL
    ORDER BY ALL
    ;
    SELECT * FROM MS_RAW.STG_META.V_PRIMARY_KEYS_TWO ORDER BY ALL;