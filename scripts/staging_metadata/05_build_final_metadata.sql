-- ============================================================================
-- SP_BUILD_FINAL_METADATA: Build STAGING_META_COLUMNS and STAGING_META_TABLES
-- ============================================================================
-- Purpose: Create final metadata tables for dbt model generation
-- Returns: VARCHAR (table/column counts and success message)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE MS_RAW.DBT_META.SP_BUILD_FINAL_METADATA()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var v_start_time = new Date();
    var v_result = '';
    var v_step = '';
    var v_column_count = 0;
    var v_table_count = 0;

    try {
        // ====================================================================
        // STEP 1: Create STAGING_META_COLUMNS table
        // ====================================================================
        v_step = 'Creating STAGING_META_COLUMNS table structure';

        snowflake.execute({sqlText: `
            CREATE OR REPLACE TABLE MS_RAW.DBT_META.STAGING_META_COLUMNS (
               -- Identity & Keys
                COLUMN_KEY              VARCHAR NOT NULL,
                SCHEMA_KEY              VARCHAR NOT NULL,
                TABLE_KEY               VARCHAR NOT NULL,
                COLUMN_NAME_KEY         VARCHAR NOT NULL,

                -- Source Metadata (MSSQL)
                MS_SCHEMA               VARCHAR,
                MS_TABLE                VARCHAR,
                MS_COLUMN               VARCHAR,
                MS_DATA_TYPE            VARCHAR,
                MS_IS_NULLABLE          BOOLEAN,
                MS_MAX_LENGTH           NUMBER,
                MS_PRECISION            NUMBER,
                MS_SCALE                NUMBER,
                MS_DEFAULT_VALUE        VARCHAR,

                -- Source Metadata (Snowflake RAW)
                SF_SCHEMA               VARCHAR NOT NULL,
                SF_TABLE                VARCHAR NOT NULL,
                SF_COLUMN               VARCHAR NOT NULL,
                SF_DATA_TYPE            VARCHAR NOT NULL,
                SF_IS_NULLABLE          BOOLEAN,
                SF_MAX_LENGTH           NUMBER,
                SF_PRECISION            NUMBER,
                SF_SCALE                NUMBER,
                SF_ORDINAL_POSITION     NUMBER,

                -- Target Staging Definition
                DBT_COLUMN_NAME         VARCHAR,
                DBT_COLUMN_KEY          VARCHAR PRIMARY KEY,
                DBT_DATA_TYPE           VARCHAR,
                DBT_IS_NULLABLE         BOOLEAN,
                DBT_TRANSFORMATION      VARCHAR,
                DBT_DESCRIPTION         VARCHAR,
                DBT_ORDINAL_POSITION    NUMBER,
                DBT_IS_EXCLUDED         BOOLEAN DEFAULT FALSE,

                -- Primary Key Metadata
                IS_PRIMARY_KEY          BOOLEAN DEFAULT FALSE,
                IS_SURROGATE_KEY        BOOLEAN DEFAULT FALSE,
                PK_CONSTRAINT_NAME      VARCHAR,
                PK_ORDINAL              NUMBER,
                PK_COLUMNS_ARRAY        ARRAY,

                -- Foreign Key Metadata
                IS_FOREIGN_KEY          BOOLEAN DEFAULT FALSE,
                FK_CONSTRAINT_NAME      VARCHAR,
                FK_PARENT_SCHEMA        VARCHAR,
                FK_PARENT_TABLE         VARCHAR,
                FK_PARENT_COLUMN        VARCHAR,
                FK_PARENT_KEY           VARCHAR,
                FK_ORDINAL              NUMBER,

                -- Column Classification
                COLUMN_SOURCE           VARCHAR(20),
                COLUMN_CATEGORY         VARCHAR(50),
                IS_NEW_COLUMN           BOOLEAN DEFAULT FALSE,
                IS_COMPUTED             BOOLEAN DEFAULT FALSE,

                -- Metadata
                NOTES                   VARCHAR,
                TAGS                    ARRAY,
                CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT              TIMESTAMP_NTZ,
                CREATED_BY              VARCHAR,
                UPDATED_BY              VARCHAR
            )
            CLUSTER BY (TABLE_KEY, DBT_ORDINAL_POSITION)
            COMMENT = 'Final column-level metadata for dbt staging model generation'
        `});

        v_result += 'Created STAGING_META_COLUMNS table structure\n';

        // ====================================================================
        // STEP 2: Load base columns from SF_COLUMNS
        // ====================================================================
        v_step = 'Loading base columns from SF_COLUMNS';

        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.STAGING_META_COLUMNS (
                COLUMN_KEY,
                SCHEMA_KEY,
                TABLE_KEY,
                COLUMN_NAME_KEY,
                MS_SCHEMA,
                MS_TABLE,
                MS_COLUMN,
                MS_DATA_TYPE,
                MS_IS_NULLABLE,
                MS_MAX_LENGTH,
                MS_PRECISION,
                MS_SCALE,
                MS_DEFAULT_VALUE,
                SF_SCHEMA,
                SF_TABLE,
                SF_COLUMN,
                SF_DATA_TYPE,
                SF_IS_NULLABLE,
                SF_MAX_LENGTH,
                SF_PRECISION,
                SF_SCALE,
                SF_ORDINAL_POSITION,
                DBT_COLUMN_NAME,
                DBT_COLUMN_KEY,
                DBT_DATA_TYPE,
                DBT_IS_NULLABLE,
                DBT_ORDINAL_POSITION,
                DBT_IS_EXCLUDED,
                COLUMN_SOURCE,
                COLUMN_CATEGORY,
                IS_NEW_COLUMN,
                CREATED_BY
            )
            SELECT
                sfc.COLUMN_KEY,
                sfc.SCHEMA_ONLY_KEY AS SCHEMA_KEY,
                sfc.TABLE_KEY,
                sfc.COLUMN_ONLY_KEY AS COLUMN_NAME_KEY,
                -- MSSQL metadata
                sfc.MS_TABLE_SCHEMA AS MS_SCHEMA,
                sfc.MS_TABLE_NAME AS MS_TABLE,
                sfc.MS_COLUMN_NAME AS MS_COLUMN,
                sfc.MS_DATA_TYPE,
                IFF(sfc.MS_IS_NULLABLE = 'YES', TRUE, FALSE) AS MS_IS_NULLABLE,
                sfc.MS_CHARACTER_MAXIMUM_LENGTH AS MS_MAX_LENGTH,
                sfc.MS_NUMERIC_PRECISION AS MS_PRECISION,
                sfc.MS_NUMERIC_SCALE AS MS_SCALE,
                sfc.MS_COLUMN_DEFAULT AS MS_DEFAULT_VALUE,
                -- Snowflake metadata
                sfc.SF_TABLE_SCHEMA AS SF_SCHEMA,
                sfc.SF_TABLE_NAME AS SF_TABLE,
                sfc.SF_COLUMN_NAME AS SF_COLUMN,
                sfc.SF_DATA_TYPE,
                IFF(sfc.SF_IS_NULLABLE = 'YES', TRUE, FALSE) AS SF_IS_NULLABLE,
                sfc.SF_CHARACTER_MAXIMUM_LENGTH AS SF_MAX_LENGTH,
                sfc.SF_NUMERIC_PRECISION AS SF_PRECISION,
                sfc.SF_NUMERIC_SCALE AS SF_SCALE,
                sfc.SF_ORDINAL_POSITION,
                -- DBT defaults (initially same as SF)
                sfc.SF_COLUMN_NAME AS DBT_COLUMN_NAME,
                sfc.TABLE_KEY || '.' || REPLACE(DBT_COLUMN_NAME, '_', '') AS DBT_COLUMN_KEY,
                sfc.SF_DATA_TYPE AS DBT_DATA_TYPE,
                IFF(sfc.SF_IS_NULLABLE = 'YES', TRUE, FALSE) AS DBT_IS_NULLABLE,
                sfc.SF_ORDINAL_POSITION AS DBT_ORDINAL_POSITION,
                FALSE AS DBT_IS_EXCLUDED,
                -- Column classification
                'MSSQL' AS COLUMN_SOURCE,
                MS_RAW.DBT_META.UDF_CLASSIFY_COLUMN_CATEGORY(
                    sfc.SF_COLUMN_NAME,
                    sfc.SF_DATA_TYPE
                ) AS COLUMN_CATEGORY,
                FALSE AS IS_NEW_COLUMN,
                CURRENT_USER() AS CREATED_BY
            FROM MS_RAW.DBT_META.SF_COLUMNS sfc
            WHERE sfc.TABLE_KEY IN 
                (SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_INGESTED_TABLES)
               AND sfc.COLUMN_KEY NOT IN 
                (SELECT COLUMN_KEY FROM MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS)
            ORDER BY sfc.COLUMN_KEY
        `});

        v_result += 'Loaded base columns from SF_COLUMNS\n';

        // ====================================================================
        // STEP 3: Apply column transformations
        // ====================================================================
        v_step = 'Applying column transformations';

        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.STAGING_META_COLUMNS (
                COLUMN_KEY,
                SCHEMA_KEY,
                TABLE_KEY,
                COLUMN_NAME_KEY,
                MS_SCHEMA,
                MS_TABLE,
                MS_COLUMN,
                MS_DATA_TYPE,
                MS_IS_NULLABLE,
                MS_MAX_LENGTH,
                MS_PRECISION,
                MS_SCALE,
                MS_DEFAULT_VALUE,
                SF_SCHEMA,
                SF_TABLE,
                SF_COLUMN,
                SF_DATA_TYPE,
                SF_IS_NULLABLE,
                SF_MAX_LENGTH,
                SF_PRECISION,
                SF_SCALE,
                SF_ORDINAL_POSITION,
                DBT_COLUMN_NAME,
                DBT_COLUMN_KEY,
                DBT_DATA_TYPE,
                DBT_IS_NULLABLE,
                DBT_ORDINAL_POSITION,
                DBT_IS_EXCLUDED,
                COLUMN_SOURCE,
                COLUMN_CATEGORY,
                IS_NEW_COLUMN,
                NOTES,
                UPDATED_AT,
                UPDATED_BY
            )
            SELECT
                sfc.COLUMN_KEY,
                sfc.SCHEMA_ONLY_KEY AS SCHEMA_KEY,
                sfc.TABLE_KEY,
                sfc.COLUMN_ONLY_KEY AS COLUMN_NAME_KEY,
                -- MSSQL metadata
                sfc.MS_TABLE_SCHEMA AS MS_SCHEMA,
                sfc.MS_TABLE_NAME AS MS_TABLE,
                sfc.MS_COLUMN_NAME AS MS_COLUMN,
                sfc.MS_DATA_TYPE,
                IFF(sfc.MS_IS_NULLABLE = 'YES', TRUE, FALSE) AS MS_IS_NULLABLE,
                sfc.MS_CHARACTER_MAXIMUM_LENGTH AS MS_MAX_LENGTH,
                sfc.MS_NUMERIC_PRECISION AS MS_PRECISION,
                sfc.MS_NUMERIC_SCALE AS MS_SCALE,
                sfc.MS_COLUMN_DEFAULT AS MS_DEFAULT_VALUE,
                -- Snowflake metadata
                sfc.SF_TABLE_SCHEMA AS SF_SCHEMA,
                sfc.SF_TABLE_NAME AS SF_TABLE,
                sfc.SF_COLUMN_NAME AS SF_COLUMN,
                sfc.SF_DATA_TYPE,
                IFF(sfc.SF_IS_NULLABLE = 'YES', TRUE, FALSE) AS SF_IS_NULLABLE,
                sfc.SF_CHARACTER_MAXIMUM_LENGTH AS SF_MAX_LENGTH,
                sfc.SF_NUMERIC_PRECISION AS SF_PRECISION,
                sfc.SF_NUMERIC_SCALE AS SF_SCALE,
                sfc.SF_ORDINAL_POSITION,
                -- DBT defaults (initially same as SF)
                ct.DBT_COLUMN_NAME,
                sfc.TABLE_KEY || '.' || ct.DBT_COLUMN_NAME AS DBT_COLUMN_KEY,
                ct.DBT_DATA_TYPE,
                IFF(sfc.SF_IS_NULLABLE = 'YES', TRUE, FALSE) AS DBT_IS_NULLABLE,
                sfc.SF_ORDINAL_POSITION AS DBT_ORDINAL_POSITION,
                FALSE AS DBT_IS_EXCLUDED,
                -- Column classification
                'MSSQL' AS COLUMN_SOURCE,
                MS_RAW.DBT_META.UDF_CLASSIFY_COLUMN_CATEGORY(
                    sfc.SF_COLUMN_NAME,
                    sfc.SF_DATA_TYPE
                ) AS COLUMN_CATEGORY,
                TRUE AS IS_NEW_COLUMN,
                ct.TRANSFORMATION_REASON AS NOTES,
                CURRENT_TIMESTAMP() AS UPDATED_AT,
                CURRENT_USER() as UPDATED_BY
            FROM MS_RAW.DBT_META.SF_COLUMNS sfc
            INNER JOIN MS_RAW.DBT_META.COLUMN_TRANSFORMATIONS ct
                ON sfc.column_key = ct.column_key
            ORDER BY DBT_COLUMN_KEY
        `});

        v_result += 'Applied column transformations\n';

        // ====================================================================
        // STEP 4: Update single-column PK metadata
        // ====================================================================
        v_step = 'Updating single-column PK metadata';

        snowflake.execute({sqlText: `
            MERGE INTO MS_RAW.DBT_META.STAGING_META_COLUMNS smc
            USING MS_RAW.DBT_META.V_SINGLE_COLUMN_PKS pk
            ON smc.COLUMN_KEY = pk.COLUMN_KEY
            WHEN MATCHED THEN UPDATE SET
                smc.IS_PRIMARY_KEY = TRUE,
                smc.IS_SURROGATE_KEY = FALSE,
                smc.PK_CONSTRAINT_NAME = pk.CONSTRAINT_NAME,
                smc.PK_ORDINAL = 1,
                smc.UPDATED_AT = CURRENT_TIMESTAMP(),
                smc.UPDATED_BY = CURRENT_USER();
        `});

        v_result += 'Updated single-column PK metadata\n';

        // ====================================================================
        // STEP 5: Update single-column FK metadata
        // ====================================================================
        v_step = 'Updating single-column FK metadata';

        snowflake.execute({sqlText: `
            MERGE INTO MS_RAW.DBT_META.STAGING_META_COLUMNS smc
            USING (
                SELECT DISTINCT
                    CHILD_COLUMN_KEY,
                    CONSTRAINT_NAME,
                    PARENT_SCHEMA,
                    PARENT_TABLE,
                    PARENT_COLUMN,
                    PARENT_COLUMN_KEY,
                    FK_ORDINAL
                FROM MS_RAW.DBT_META.V_FK_RELATIONSHIPS
                WHERE fk_column_count = 1
            ) fk
            ON smc.COLUMN_KEY = fk.CHILD_COLUMN_KEY
            WHEN MATCHED THEN UPDATE SET
                smc.IS_FOREIGN_KEY = TRUE,
                smc.FK_CONSTRAINT_NAME = fk.CONSTRAINT_NAME,
                smc.FK_PARENT_SCHEMA = fk.PARENT_SCHEMA,
                smc.FK_PARENT_TABLE = fk.PARENT_TABLE,
                smc.FK_PARENT_COLUMN = fk.PARENT_COLUMN,
                smc.FK_PARENT_KEY = fk.PARENT_COLUMN_KEY,
                smc.FK_ORDINAL = fk.FK_ORDINAL,
                smc.UPDATED_AT = CURRENT_TIMESTAMP(),
                smc.UPDATED_BY = CURRENT_USER()
        `});

        v_result += 'Updated single-column FK metadata\n';

        // ====================================================================
        // STEP 6: Insert surrogate key columns
        // ====================================================================
        v_step = 'Inserting surrogate key columns';

        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.STAGING_META_COLUMNS (
                COLUMN_KEY,
                SCHEMA_KEY,
                TABLE_KEY,
                COLUMN_NAME_KEY,
                SF_SCHEMA,
                SF_TABLE,
                SF_COLUMN,
                SF_DATA_TYPE,
                SF_IS_NULLABLE,
                SF_ORDINAL_POSITION,
                DBT_COLUMN_NAME,
                DBT_COLUMN_KEY,
                DBT_DATA_TYPE,
                DBT_IS_NULLABLE,
                DBT_TRANSFORMATION,
                DBT_ORDINAL_POSITION,
                IS_PRIMARY_KEY,
                IS_SURROGATE_KEY,
                IS_FOREIGN_KEY,
                FK_PARENT_KEY,
                COLUMN_SOURCE,
                COLUMN_CATEGORY,
                IS_NEW_COLUMN,
                NOTES,
                CREATED_BY
            )
            SELECT
                skp.TABLE_KEY || '.' || UPPER(REPLACE(skp.NEW_COLUMN_NAME, '_', '')) AS COLUMN_KEY,
                skp.SCHEMA_ONLY_KEY AS SCHEMA_KEY,
                skp.TABLE_KEY,
                UPPER(REPLACE(skp.NEW_COLUMN_NAME, '_', '')) AS COLUMN_NAME_KEY,
                skp.SCHEMA_ONLY_KEY AS SF_SCHEMA,
                skp.SF_TABLE_NAME AS SF_TABLE,
                skp.NEW_COLUMN_NAME AS SF_COLUMN,
                skp.NEW_DATA_TYPE AS SF_DATA_TYPE,
                IFF(skp.IS_PRIMARY_KEY, FALSE, TRUE) AS SF_IS_NULLABLE,
                skp.ORDINAL_POSITION AS SF_ORDINAL_POSITION,
                skp.NEW_COLUMN_NAME AS DBT_COLUMN_NAME,
                COLUMN_KEY AS DBT_COLUMN_KEY,
                skp.NEW_DATA_TYPE AS DBT_DATA_TYPE,
                IFF(skp.IS_PRIMARY_KEY, FALSE, TRUE) AS DBT_IS_NULLABLE,
                skp.TRANSFORMATION AS DBT_TRANSFORMATION,
                skp.ORDINAL_POSITION AS DBT_ORDINAL_POSITION,
                skp.IS_PRIMARY_KEY,
                skp.IS_SURROGATE_KEY,
                skp.IS_FOREIGN_KEY,
                skp.FK_PARENT_TABLE_KEY || '.' || UPPER(REPLACE(skp.NEW_COLUMN_NAME, '_', '')) AS FK_PARENT_KEY,
                skp.COLUMN_SOURCE,
                'ID' AS COLUMN_CATEGORY,
                TRUE AS IS_NEW_COLUMN,
                skp.NOTES,
                CURRENT_USER() AS CREATED_BY
            FROM MS_RAW.DBT_META.V_SURROGATE_KEY_PLAN skp
        `});

        v_result += 'Inserted surrogate key columns\n';

        // ====================================================================
        // STEP 7: Clear original composite FK metadata
        // ====================================================================
        v_step = 'Clearing original composite FK metadata';

        snowflake.execute({sqlText: `
            UPDATE MS_RAW.DBT_META.STAGING_META_COLUMNS smc
            SET
                IS_FOREIGN_KEY = FALSE,
                FK_CONSTRAINT_NAME = NULL,
                FK_PARENT_SCHEMA = NULL,
                FK_PARENT_TABLE = NULL,
                FK_PARENT_COLUMN = NULL,
                FK_PARENT_KEY = NULL,
                FK_ORDINAL = NULL,
                NOTES = COALESCE(NOTES || '; ', '') ||
                        'Was part of composite FK, now replaced by surrogate FK',
                UPDATED_AT = CURRENT_TIMESTAMP(),
                UPDATED_BY = CURRENT_USER()
            WHERE COLUMN_KEY IN (
                SELECT DISTINCT fk.CHILD_COLUMN_KEY
                FROM MS_RAW.DBT_META.V_FK_RELATIONSHIPS fk
                WHERE fk.fk_column_count >= 2
                  AND fk.PARENT_TABLE_KEY IN (
                      SELECT TABLE_KEY FROM MS_RAW.DBT_META.V_COMPOSITE_PKS
                  )
            )
        `});

        v_result += 'Cleared original composite FK metadata\n';

        // ====================================================================
        // STEP 8: Get column count
        // ====================================================================
        var count_rs = snowflake.execute({
            sqlText: "SELECT COUNT(*) AS CNT FROM MS_RAW.DBT_META.STAGING_META_COLUMNS"
        });
        if (count_rs.next()) {
            v_column_count = count_rs.getColumnValue('CNT');
        }

        v_result += 'Total columns in STAGING_META_COLUMNS: ' + v_column_count + '\n';

        // ====================================================================
        // STEP 9: Create STAGING_META_TABLES
        // ====================================================================
        v_step = 'Creating STAGING_META_TABLES';

        snowflake.execute({sqlText: `
            CREATE OR REPLACE TABLE MS_RAW.DBT_META.STAGING_META_TABLES (
                TABLE_KEY               VARCHAR PRIMARY KEY,
                SCHEMA_KEY              VARCHAR NOT NULL,
                TABLE_NAME_KEY          VARCHAR NOT NULL,

                -- Source Metadata
                MS_SCHEMA               VARCHAR,
                MS_TABLE                VARCHAR,
                SF_SCHEMA               VARCHAR NOT NULL,
                SF_TABLE                VARCHAR NOT NULL,
                DBT_MODEL_NAME          VARCHAR,

                -- Table Classification
                TABLE_TYPE              VARCHAR(50),
                
                HAS_COMPOSITE_PK        BOOLEAN DEFAULT FALSE,
                HAS_SURROGATE_PK        BOOLEAN DEFAULT FALSE,
                HAS_NATURAL_PK          BOOLEAN DEFAULT FALSE,
                HAS_NO_PK               BOOLEAN DEFAULT FALSE,

                -- Column Statistics
                TOTAL_COLUMNS           NUMBER,
                SOURCE_COLUMNS          NUMBER,
                DERIVED_COLUMNS         NUMBER,
                SURROGATE_COLUMNS       NUMBER,
                EXCLUDED_COLUMNS        NUMBER,

                -- Key Statistics
                PK_COLUMN_COUNT         NUMBER,
                PK_COLUMN               VARCHAR,
                ORIGINAL_PK_COLUMNS     ARRAY,
                SURROGATE_PK_NAME       VARCHAR,
                FK_COUNT                NUMBER,
                CHILD_TABLE_COUNT       NUMBER,

                -- Data Type Summary
                TIMESTAMP_COLUMNS       NUMBER,
                DATE_COLUMNS            NUMBER,
                FLAG_COLUMNS            NUMBER,
                ID_COLUMNS              NUMBER,
                TEXT_COLUMNS            NUMBER,
                NUMERIC_COLUMNS         NUMBER,

                -- Metadata
                NOTES                   VARCHAR,
                TAGS                    ARRAY,
                DBT_MATERIALIZATION     VARCHAR DEFAULT 'view',
                ROW_COUNT_ESTIMATE      NUMBER,
                CREATED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                UPDATED_AT              TIMESTAMP_NTZ
            )
            CLUSTER BY (SCHEMA_KEY, TABLE_NAME_KEY)
            COMMENT = 'Final table-level metadata for dbt staging model generation'
        `});

        // ====================================================================
        // STEP 10: Populate STAGING_META_TABLES
        // ====================================================================
        v_step = 'Populating STAGING_META_TABLES';

        snowflake.execute({sqlText: `
            INSERT INTO MS_RAW.DBT_META.STAGING_META_TABLES
            SELECT
                smc.TABLE_KEY,
                smc.SCHEMA_KEY,
                MAX(smc.SCHEMA_KEY || '.' || smc.SF_TABLE) AS TABLE_NAME_KEY,
                MAX(smc.MS_SCHEMA) AS MS_SCHEMA,
                MAX(smc.MS_TABLE) AS MS_TABLE,
                MAX(smc.SF_SCHEMA) AS SF_SCHEMA,
                MAX(smc.SF_TABLE) AS SF_TABLE,
                'stg_reedonline__' || LOWER(MAX(smc.SF_SCHEMA)) || '__' || LOWER(MAX(smc.SF_TABLE)) AS DBT_MODEL_NAME,

                -- Table Classification
                NULL AS TABLE_TYPE,
                IFF(cpk.TABLE_KEY IS NOT NULL, TRUE, FALSE) AS HAS_COMPOSITE_PK,
                IFF(MAX(smc.IS_SURROGATE_KEY) = TRUE, TRUE, FALSE) AS HAS_SURROGATE_PK,
                IFF(SUM(IFF(smc.IS_PRIMARY_KEY AND NOT smc.IS_SURROGATE_KEY, 1, 0)) > 0, TRUE, FALSE) AS HAS_NATURAL_PK,
                IFF(SUM(IFF(smc.IS_PRIMARY_KEY, 1, 0)) = 0, TRUE, FALSE) AS HAS_NO_PK,

                -- Column Statistics
                COUNT(*) AS TOTAL_COLUMNS,
                SUM(IFF(smc.COLUMN_SOURCE = 'MSSQL', 1, 0)) AS SOURCE_COLUMNS,
                SUM(IFF(smc.COLUMN_SOURCE = 'DERIVED', 1, 0)) AS DERIVED_COLUMNS,
                SUM(IFF(smc.COLUMN_SOURCE = 'SURROGATE', 1, 0)) AS SURROGATE_COLUMNS,
                SUM(IFF(smc.DBT_IS_EXCLUDED, 1, 0)) AS EXCLUDED_COLUMNS,

                -- Key Statistics
                SUM(IFF(smc.IS_PRIMARY_KEY, 1, 0)) AS PK_COLUMN_COUNT,
                MAX(CASE WHEN smc.IS_PRIMARY_KEY THEN smc.SF_COLUMN END) AS PK_COLUMN,
                cpk.PK_COLUMNS AS ORIGINAL_PK_COLUMNS,
                cpk.SURROGATE_KEY_NAME AS SURROGATE_PK_NAME,
                SUM(IFF(smc.IS_FOREIGN_KEY, 1, 0)) AS FK_COUNT,
                0 AS CHILD_TABLE_COUNT, -- Calculated in next step

                -- Data Type Summary
                SUM(IFF(smc.COLUMN_CATEGORY = 'TIMESTAMP', 1, 0)) AS TIMESTAMP_COLUMNS,
                SUM(IFF(smc.COLUMN_CATEGORY = 'DATE', 1, 0)) AS DATE_COLUMNS,
                SUM(IFF(smc.COLUMN_CATEGORY = 'FLAG', 1, 0)) AS FLAG_COLUMNS,
                SUM(IFF(smc.COLUMN_CATEGORY = 'ID', 1, 0)) AS ID_COLUMNS,
                SUM(IFF(smc.COLUMN_CATEGORY = 'TEXT', 1, 0)) AS TEXT_COLUMNS,
                SUM(IFF(smc.COLUMN_CATEGORY = 'NUMERIC', 1, 0)) AS NUMERIC_COLUMNS,

                -- Metadata
                NULL AS NOTES,
                NULL AS TAGS,
                'view' AS DBT_MATERIALIZATION,
                NULL AS ROW_COUNT_ESTIMATE,
                CURRENT_TIMESTAMP() AS CREATED_AT,
                NULL AS UPDATED_AT
            FROM MS_RAW.DBT_META.STAGING_META_COLUMNS smc
            LEFT JOIN MS_RAW.DBT_META.V_COMPOSITE_PKS cpk
                ON smc.TABLE_KEY = cpk.TABLE_KEY
            WHERE NOT smc.DBT_IS_EXCLUDED
            GROUP BY ALL
            ORDER BY smc.TABLE_KEY
        `});

        // Update child table counts
        snowflake.execute({sqlText: `
            UPDATE MS_RAW.DBT_META.STAGING_META_TABLES smt
            SET CHILD_TABLE_COUNT = (
                SELECT COUNT(DISTINCT CHILD_TABLE_KEY)
                FROM MS_RAW.DBT_META.V_FK_RELATIONSHIPS fk
                WHERE fk.PARENT_TABLE_KEY = smt.TABLE_KEY
                ),
                PK_COLUMN = CASE WHEN PK_COLUMN_COUNT = 1 THEN PK_COLUMN END
                
        `});

        var table_count_rs = snowflake.execute({
            sqlText: "SELECT COUNT(*) AS CNT FROM MS_RAW.DBT_META.STAGING_META_TABLES"
        });
        if (table_count_rs.next()) {
            v_table_count = table_count_rs.getColumnValue('CNT');
        }

        v_result += 'Created STAGING_META_TABLES: ' + v_table_count + ' tables\n';

        // ====================================================================
        // STEP 11: Return Summary
        // ====================================================================
        var duration = (new Date() - v_start_time) / 1000;
        v_result += '\nFinal metadata build completed in ' + duration.toFixed(2) + ' seconds';

        return v_result;

    } catch (err) {
        return 'ERROR in step: ' + v_step + '\nError: ' + err.message + '\nStack: ' + err.stack;
    }
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE MS_RAW.DBT_META.SP_BUILD_FINAL_METADATA() TO ROLE SYSADMIN;

-- ============================================================================
-- Test execution
-- ============================================================================
-- CALL MS_RAW.DBT_META.SP_BUILD_FINAL_METADATA();
