-- ============================================================================
-- SP_BUILD_STAGING_METADATA: Main orchestrator procedure
-- ============================================================================
-- Purpose: Execute complete metadata pipeline in correct order with logging
-- Parameters:
--   p_rebuild_foundation: If TRUE, recreates base structures (default: FALSE)
--   p_exclude_tables: Comma-separated list of tables to exclude (default: 'PRICEBOOKENTRY')
--   p_validate_results: If TRUE, runs validation checks (default: TRUE)
-- Returns: VARCHAR (execution summary with timing)
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE PROCEDURE MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(
    p_rebuild_foundation BOOLEAN DEFAULT FALSE,
    p_exclude_tables VARCHAR DEFAULT 'PRICEBOOKENTRY',
    p_validate_results BOOLEAN DEFAULT TRUE
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var v_start_time = new Date();
    var v_log_id = null;
    var v_result = '';
    var v_step = '';
    var v_validation_passed = false;
    var v_tables_created = 0;
    var v_columns_created = 0;

    try {
        v_result += '=========================================\\n';
        v_result += 'STAGING METADATA PIPELINE EXECUTION\\n';
        v_result += '=========================================\\n';
        v_result += 'Started: ' + v_start_time.toISOString() + '\\n';
        v_result += 'Rebuild Foundation: ' + P_REBUILD_FOUNDATION + '\\n';
        v_result += 'Exclude Tables: ' + P_EXCLUDE_TABLES + '\\n';
        v_result += 'Validate Results: ' + P_VALIDATE_RESULTS + '\\n';
        v_result += '=========================================\\n\\n';

        // ====================================================================
        // LOG START
        // ====================================================================
        v_step = 'Logging execution start';

        var log_stmt = snowflake.createStatement({
            sqlText: `
                INSERT INTO MS_RAW.DBT_META.METADATA_BUILD_LOG (
                    EXECUTION_START_TIME,
                    PROCEDURE_NAME,
                    REBUILD_FOUNDATION,
                    EXCLUDE_TABLES,
                    VALIDATION_PASSED,
                    STATUS
                ) VALUES (?, 'SP_BUILD_STAGING_METADATA', ?, ?, NULL, 'RUNNING')
            `,
            binds: [v_start_time, P_REBUILD_FOUNDATION, P_EXCLUDE_TABLES]
        });

        log_stmt.execute();

        var log_id_rs = snowflake.execute({
            sqlText: "SELECT MAX(LOG_ID) AS LOG_ID FROM MS_RAW.DBT_META.METADATA_BUILD_LOG"
        });
        if (log_id_rs.next()) {
            v_log_id = log_id_rs.getColumnValue('LOG_ID');
        }

        // ====================================================================
        // STAGE 1: Foundation Setup (Optional)
        // ====================================================================
        if (P_REBUILD_FOUNDATION) {
            v_step = 'Stage 1: Foundation Setup';
            v_result += '\\n[STAGE 1] Foundation Setup\\n';
            v_result += '-------------------------------------------\\n';

            var foundation_rs = snowflake.execute({
                sqlText: "CALL MS_RAW.DBT_META.SP_SETUP_FOUNDATION()"
            });

            if (foundation_rs.next()) {
                var foundation_result = foundation_rs.getColumnValue(1);
                v_result += foundation_result + '\\n';
            }
        } else {
            v_result += '\\n[STAGE 1] Foundation Setup - SKIPPED\\n';
        }

        // ====================================================================
        // STAGE 2: Metadata Integration
        // ====================================================================
        v_step = 'Stage 2: Metadata Integration';
        v_result += '\\n[STAGE 2] Metadata Integration\\n';
        v_result += '-------------------------------------------\\n';

        var metadata_rs = snowflake.execute({
            sqlText: "CALL MS_RAW.DBT_META.SP_JOIN_METADATA(?)",
            binds: [P_EXCLUDE_TABLES]
        });

        if (metadata_rs.next()) {
            var metadata_result = metadata_rs.getColumnValue(1);
            v_result += metadata_result + '\\n';
        }

        // ====================================================================
        // STAGE 3: Column Transformations
        // ====================================================================
        v_step = 'Stage 3: Column Transformations';
        v_result += '\\n[STAGE 3] Column Transformations\\n';
        v_result += '-------------------------------------------\\n';

        var normalize_rs = snowflake.execute({
            sqlText: "CALL MS_RAW.DBT_META.SP_NORMALIZE_COLUMNS()"
        });

        if (normalize_rs.next()) {
            var normalize_result = normalize_rs.getColumnValue(1);
            v_result += normalize_result + '\\n';
        }

        // ====================================================================
        // STAGE 4: Relationship Analysis
        // ====================================================================
        v_step = 'Stage 4: Relationship Analysis';
        v_result += '\\n[STAGE 4] Relationship Analysis\\n';
        v_result += '-------------------------------------------\\n';

        var relationships_rs = snowflake.execute({
            sqlText: "CALL MS_RAW.DBT_META.SP_ANALYZE_RELATIONSHIPS()"
        });

        if (relationships_rs.next()) {
            var relationships_result = relationships_rs.getColumnValue(1);
            v_result += relationships_result + '\\n';
        }

        // ====================================================================
        // STAGE 5: Build Final Metadata
        // ====================================================================
        v_step = 'Stage 5: Build Final Metadata';
        v_result += '\\n[STAGE 5] Build Final Metadata\\n';
        v_result += '-------------------------------------------\\n';

        var final_rs = snowflake.execute({
            sqlText: "CALL MS_RAW.DBT_META.SP_BUILD_FINAL_METADATA()"
        });

        if (final_rs.next()) {
            var final_result = final_rs.getColumnValue(1);
            v_result += final_result + '\\n';
        }

        // Get row counts
        var table_count_rs = snowflake.execute({
            sqlText: "SELECT COUNT(*) AS CNT FROM MS_RAW.DBT_META.STAGING_META_TABLES"
        });
        if (table_count_rs.next()) {
            v_tables_created = table_count_rs.getColumnValue('CNT');
        }

        var column_count_rs = snowflake.execute({
            sqlText: "SELECT COUNT(*) AS CNT FROM MS_RAW.DBT_META.STAGING_META_COLUMNS"
        });
        if (column_count_rs.next()) {
            v_columns_created = column_count_rs.getColumnValue('CNT');
        }

        // ====================================================================
        // STAGE 6: Validation (Optional)
        // ====================================================================
        if (P_VALIDATE_RESULTS) {
            v_step = 'Stage 6: Validation';
            v_result += '\\n[STAGE 6] Validation\\n';
            v_result += '-------------------------------------------\\n';

            var validate_rs = snowflake.execute({
                sqlText: "CALL MS_RAW.DBT_META.SP_VALIDATE_METADATA()"
            });

            if (validate_rs.next()) {
                var validate_result = validate_rs.getColumnValue(1);
                v_result += validate_result + '\\n';
                v_validation_passed = validate_result.includes('STATUS: PASSED');
            }
        } else {
            v_result += '\\n[STAGE 6] Validation - SKIPPED\\n';
            v_validation_passed = null;
        }

        // ====================================================================
        // FINAL SUMMARY
        // ====================================================================
        var v_end_time = new Date();
        var duration = (v_end_time - v_start_time) / 1000;

        v_result += '\\n=========================================\\n';
        v_result += 'PIPELINE EXECUTION COMPLETE\\n';
        v_result += '=========================================\\n';
        v_result += 'Duration: ' + duration.toFixed(2) + ' seconds\\n';
        v_result += 'Tables Created: ' + v_tables_created + '\\n';
        v_result += 'Columns Created: ' + v_columns_created + '\\n';
        v_result += 'Validation: ' + (v_validation_passed ? 'PASSED' : (v_validation_passed === false ? 'FAILED' : 'SKIPPED')) + '\\n';
        v_result += '=========================================\\n';

        // ====================================================================
        // LOG COMPLETION
        // ====================================================================
        snowflake.execute({
            sqlText: `
                UPDATE MS_RAW.DBT_META.METADATA_BUILD_LOG
                SET
                    EXECUTION_END_TIME = ?,
                    DURATION_SECONDS = ?,
                    VALIDATION_PASSED = ?,
                    STATUS = 'SUCCESS',
                    RESULT_SUMMARY = ?,
                    TABLES_CREATED = ?,
                    COLUMNS_CREATED = ?
                WHERE LOG_ID = ?
            `,
            binds: [v_end_time, duration, v_validation_passed, v_result, v_tables_created, v_columns_created, v_log_id]
        });

        return v_result;

    } catch (err) {
        var error_message = 'ERROR in ' + v_step + ': ' + err.message + '\\nStack: ' + err.stack;

        // Log error
        if (v_log_id) {
            try {
                snowflake.execute({
                    sqlText: `
                        UPDATE MS_RAW.DBT_META.METADATA_BUILD_LOG
                        SET
                            EXECUTION_END_TIME = CURRENT_TIMESTAMP(),
                            STATUS = 'FAILED',
                            ERROR_MESSAGE = ?
                        WHERE LOG_ID = ?
                    `,
                    binds: [error_message, v_log_id]
                });
            } catch (log_err) {
                // Ignore logging errors
            }
        }

        return error_message;
    }
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(BOOLEAN, VARCHAR, BOOLEAN) TO ROLE SYSADMIN;

-- ============================================================================
-- Usage Examples
-- ============================================================================

-- Full rebuild (first time execution)
-- CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(TRUE, 'PRICEBOOKENTRY', TRUE);

-- Incremental update (foundation already exists)
-- CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(FALSE, 'PRICEBOOKENTRY', TRUE);

-- Quick rebuild without validation
-- CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(FALSE, 'PRICEBOOKENTRY', FALSE);

-- Check execution history
-- SELECT * FROM MS_RAW.DBT_META.METADATA_BUILD_LOG ORDER BY EXECUTION_START_TIME DESC LIMIT 10;
