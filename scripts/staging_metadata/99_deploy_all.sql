-- ============================================================================
-- DEPLOY ALL STORED PROCEDURES
-- ============================================================================
-- Purpose: Deploy all metadata pipeline stored procedures in correct order
-- Usage: Execute this script once to create all procedures
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE MS_RAW;
USE SCHEMA DBT_META;

-- Set script variables
SET deployment_start = CURRENT_TIMESTAMP();

SELECT 'Starting deployment at ' || :deployment_start AS deployment_status;

-- ============================================================================
-- STEP 1: Foundation Setup Procedure
-- ============================================================================
SELECT 'Deploying SP_SETUP_FOUNDATION...' AS status;
!source 01_foundation.sql

-- ============================================================================
-- STEP 2: Metadata Integration Procedure
-- ============================================================================
SELECT 'Deploying SP_JOIN_METADATA...' AS status;
!source 02_metadata_integration.sql

-- ============================================================================
-- STEP 3: Column Transformation Procedure
-- ============================================================================
SELECT 'Deploying SP_NORMALIZE_COLUMNS...' AS status;
!source 03_column_transformations.sql

-- ============================================================================
-- STEP 4: Relationship Analysis Procedure
-- ============================================================================
SELECT 'Deploying SP_ANALYZE_RELATIONSHIPS...' AS status;
!source 04_relationship_analysis.sql

-- ============================================================================
-- STEP 5: Build Final Metadata Procedure
-- ============================================================================
SELECT 'Deploying SP_BUILD_FINAL_METADATA...' AS status;
!source 05_build_final_metadata.sql

-- ============================================================================
-- STEP 6: Validation Procedure
-- ============================================================================
SELECT 'Deploying SP_VALIDATE_METADATA...' AS status;
!source 06_validation.sql

-- ============================================================================
-- STEP 7: Orchestrator Procedure
-- ============================================================================
SELECT 'Deploying SP_BUILD_STAGING_METADATA (orchestrator)...' AS status;
!source 07_orchestrator.sql

-- ============================================================================
-- VERIFY DEPLOYMENT
-- ============================================================================
SELECT 'Verifying deployment...' AS status;

SELECT
    procedure_name,
    procedure_language,
    created,
    last_altered
FROM MS_RAW.INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema = 'DBT_META'
  AND procedure_name LIKE 'SP_%'
ORDER BY procedure_name;

SELECT 'Deployment complete! Duration: '
    || DATEDIFF('second', :deployment_start, CURRENT_TIMESTAMP())
    || ' seconds' AS deployment_status;

-- ============================================================================
-- NEXT STEPS
-- ============================================================================
SELECT '
=========================================
DEPLOYMENT COMPLETE!
=========================================

Next steps:

1. Execute the pipeline:
   CALL MS_RAW.DBT_META.SP_BUILD_STAGING_METADATA(TRUE, ''PRICEBOOKENTRY'', TRUE);

2. Query the results:
   SELECT * FROM MS_RAW.DBT_META.STAGING_META_TABLES;
   SELECT * FROM MS_RAW.DBT_META.STAGING_META_COLUMNS;

3. Check execution logs:
   SELECT * FROM MS_RAW.DBT_META.METADATA_BUILD_LOG
   ORDER BY EXECUTION_START_TIME DESC;

4. For help, see README.md in scripts/staging_metadata/

=========================================
' AS next_steps;
