-- SQLSERVER DEV [Reed Online]
/*
WITH idx AS (
    SELECT
        s.name        AS table_schema,
        t.name        AS table_name,
        i.name        AS index_name,
        i.is_unique,
        i.is_primary_key,
        i.type_desc
    FROM sys.indexes i
    JOIN sys.tables t
      ON t.object_id = i.object_id
    JOIN sys.schemas s
      ON s.schema_id = t.schema_id
    WHERE i.is_unique = 1
      AND s.name IN ('dbo','DuplicateJobService','JobImport','Jobs')
      AND SUBSTRING(t.name,1,1) != '_'
      AND t.name NOT LIKE 'zKill%'
      AND t.name NOT LIKE 'sys%'
),
pk AS (
    SELECT
        table_schema,
        table_name,
        index_name
   FROM idx
   WHERE is_primary_key = 1
)
SELECT idx.*, pk.index_name AS pk_name
FROM idx
LEFT JOIN pk
  ON idx.table_schema = pk.table_schema
 AND idx.table_name   = pk.table_name
ORDER BY 1,2,3
;

-- EXPORT to ms_indexes.csv

CREATE OR REPLACE TABLE MS_RAW.STG_META.MS_INDEXES (
    table_schema VARCHAR,
    table_name VARCHAR,
    index_name VARCHAR,
    is_unique BOOLEAN,
    is_primary_key BOOLEAN,
    type_desc VARCHAR,
    pk_name VARCHAR
);

-- INGEST ms_indexes.csv INTO MS_RAW.STG_META.MS_INDEXES

-- delete INDEXES for unknown tables
DELETE FROM MS_RAW.STG_META.MS_INDEXES i
WHERE NOT EXISTS (SELECT 1 FROM MS_RAW.STG_META.SF_TABLES s
                  WHERE i.table_schema = s.ms_table_schema
                    AND i.table_name = s.ms_table_name)
;

-- keep only (IS_UNIQUE OR IS_PRIMARY_KEY) INDEXES
DELETE FROM MS_RAW.STG_META.MS_INDEXES
WHERE NOT (IS_UNIQUE OR IS_PRIMARY_KEY)
;

UPDATE MS_RAW.STG_META.MS_INDEXES I
   SET PK_NAME = D.INDEX_NAME
FROM MS_RAW.STG_META.MS_INDEXES D
WHERE D.IS_PRIMARY_KEY
  AND D.TABLE_SCHEMA = I.TABLE_SCHEMA
  AND D.TABLE_NAME = I.TABLE_NAME
;
*/
-- 338
SELECT * FROM MS_RAW.STG_META.MS_INDEXES
ORDER BY ALL;

