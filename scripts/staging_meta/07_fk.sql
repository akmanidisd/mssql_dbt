CREATE OR REPLACE VIEW MS_RAW.STG_META.V_SECONDARY_KEYS AS
WITH fk_cols AS (
    SELECT
        tc.CONSTRAINT_NAME,
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        kcu.COLUMN_NAME,
        sfc.SF_TABLE_SCHEMA,
        sfc.SF_TABLE_NAME,
        sfc.SF_COLUMN_NAME,
        ROW_NUMBER() OVER (
            PARTITION BY tc.CONSTRAINT_NAME
            ORDER BY kcu.ORDINAL_POSITION
        ) AS column_ordinal
    FROM ROL_RAW.REEDONLINE_META.TABLE_CONSTRAINTS tc
    JOIN ROL_RAW.REEDONLINE_META.KEY_COLUMN_USAGE kcu
      ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
     AND tc.TABLE_SCHEMA     = kcu.TABLE_SCHEMA
    JOIN MS_RAW.STG_META.SF_COLUMNS sfc
      ON tc.TABLE_SCHEMA     = sfc.MS_TABLE_SCHEMA
     AND tc.TABLE_NAME       = sfc.MS_TABLE_NAME
     AND kcu.COLUMN_NAME     = sfc.MS_COLUMN_NAME
    WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
    ORDER BY ALL
),
pk_cols AS (
    SELECT
        tc.CONSTRAINT_NAME,
        tc.TABLE_SCHEMA,
        tc.TABLE_NAME,
        kcu.COLUMN_NAME,
        sfc.SF_TABLE_SCHEMA,
        sfc.SF_TABLE_NAME,
        sfc.SF_COLUMN_NAME,
        ROW_NUMBER() OVER (
            PARTITION BY tc.CONSTRAINT_NAME
            ORDER BY kcu.ORDINAL_POSITION
        ) AS column_ordinal,
        COUNT(*) OVER (
            PARTITION BY tc.CONSTRAINT_NAME
        ) AS total_columns
    FROM ROL_RAW.REEDONLINE_META.TABLE_CONSTRAINTS tc
    JOIN ROL_RAW.REEDONLINE_META.KEY_COLUMN_USAGE kcu
      ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
     AND tc.TABLE_SCHEMA     = kcu.TABLE_SCHEMA
    JOIN MS_RAW.STG_META.SF_COLUMNS sfc
      ON tc.TABLE_SCHEMA     = sfc.MS_TABLE_SCHEMA
     AND tc.TABLE_NAME       = sfc.MS_TABLE_NAME
     AND kcu.COLUMN_NAME     = sfc.MS_COLUMN_NAME
    WHERE tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
    ORDER BY 2,3,5
)
SELECT
    fk_cols.SF_TABLE_SCHEMA   AS fk_schema,
    fk_cols.SF_TABLE_NAME     AS fk_table,
    fk_cols.SF_COLUMN_NAME    AS fk_column,
    fk_cols.CONSTRAINT_NAME   AS fk_constraint_name,
    pk_cols.SF_TABLE_SCHEMA   AS pk_schema,
    pk_cols.SF_TABLE_NAME     AS pk_table,
    pk_cols.SF_COLUMN_NAME    AS pk_column,
    pk_cols.CONSTRAINT_NAME   AS pk_constraint_name,
    pk_cols.column_ordinal    AS pk_ordinal_position,
    pk_cols.total_columns     AS pk_total_columns
FROM ROL_RAW.REEDONLINE_META.REFERENTIAL_CONSTRAINTS rc
JOIN fk_cols
  ON rc.CONSTRAINT_NAME = fk_cols.CONSTRAINT_NAME
JOIN pk_cols
  ON rc.UNIQUE_CONSTRAINT_NAME = pk_cols.CONSTRAINT_NAME
 AND fk_cols.column_ordinal    = pk_cols.column_ordinal
ORDER BY
    fk_schema,
    fk_table,
    fk_constraint_name,
    pk_ordinal_position
;

SELECT * 
FROM MS_RAW.STG_META.V_SECONDARY_KEYS
ORDER BY ALL
;