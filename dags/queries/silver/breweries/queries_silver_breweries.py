SILVER_TRANSFORMATION_SQL = """
    WITH bronze_incremental_latest AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY bronze_file_timestamp DESC) as rn
        FROM
            bronze_raw_view
        WHERE
            bronze_file_timestamp > '{start_timestamp_for_bronze_read}'
    )
    SELECT
        TRIM(CAST(id AS STRING)) AS id,
        LOWER(TRIM(CAST(name AS STRING))) AS name,
        LOWER(TRIM(CAST(brewery_type AS STRING))) AS brewery_type,
        LOWER(TRIM(CAST(street AS STRING))) AS street,
        LOWER(TRIM(CAST(address_2 AS STRING))) AS address_2,
        LOWER(TRIM(CAST(address_3 AS STRING))) AS address_3,
        LOWER(TRIM(CAST(city AS STRING))) AS city,
        LOWER(TRIM(CAST(state AS STRING))) AS state,
        LOWER(TRIM(CAST(postal_code AS STRING))) AS postal_code,
        LOWER(TRIM(CAST(country AS STRING))) AS country, -- Manter como 'country'
        CAST(longitude AS DOUBLE) AS longitude,
        CAST(latitude AS DOUBLE) AS latitude,
        LOWER(TRIM(CAST(phone AS STRING))) AS phone,
        LOWER(TRIM(CAST(website_url AS STRING))) AS website_url,
        COALESCE(NULLIF(LOWER(TRIM(CAST(state_province AS STRING))), ''), 'unknown') AS state_province,
        bronze_file_timestamp
        -- is_deleted, created_at, updated_at removidos
    FROM
        bronze_incremental_latest
    WHERE
        rn = 1 -- Seleciona apenas a última versão de cada ID do lote incremental
"""