SILVER_TRANSFORMATION_SQL = """
    WITH latest_overall_timestamp_cte AS (
        SELECT
            MAX(bronze_file_timestamp) AS max_timestamp
        FROM
            bronze_raw_view
        WHERE
            bronze_file_timestamp > '{start_timestamp_for_bronze_read}'
    ),
    bronze_snapshot AS (
        SELECT
            b.*,
            ROW_NUMBER() OVER (PARTITION BY b.id ORDER BY b.bronze_file_timestamp DESC) as rn
        FROM
            bronze_raw_view b
        INNER JOIN
            latest_overall_timestamp_cte l
        ON
            b.bronze_file_timestamp = l.max_timestamp
    )
    SELECT
        TRIM(CAST(id AS STRING)) AS id,
        LOWER(TRIM(CAST(name AS STRING))) AS name,
        LOWER(TRIM(CAST(brewery_type AS STRING))) AS brewery_type,
        LOWER(TRIM(CAST(street AS STRING))) AS street,
        LOWER(TRIM(CAST(address_2 AS STRING))) AS address_2,
        LOWER(TRIM(CAST(address_3 AS STRING))) AS address_3,
        LOWER(TRIM(CAST(city AS STRING))) AS city,
        COALESCE(NULLIF(LOWER(TRIM(CAST(state AS STRING))), ''), 'unknown') AS state,
        LOWER(TRIM(CAST(postal_code AS STRING))) AS postal_code,
        -- Início da limpeza para a coluna de partição 'country' com tratamento para vazio/nulo
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                COALESCE(NULLIF(LOWER(TRIM(CAST(country AS STRING))), ''), 'unknown'), -- Adicionado COALESCE e NULLIF aqui
                '[^a-z0-9]+',
                '_'
            ),
            '_+',
            '_'
        ) AS country,
        -- Fim da limpeza para a coluna de partição 'country'
        CAST(longitude AS DOUBLE) AS longitude,
        CAST(latitude AS DOUBLE) AS latitude,
        LOWER(TRIM(CAST(phone AS STRING))) AS phone,
        LOWER(TRIM(CAST(website_url AS STRING))) AS website_url,
        COALESCE(NULLIF(LOWER(TRIM(CAST(state_province AS STRING))), ''), 'unknown') AS state_province,
        bronze_file_timestamp
    FROM
        bronze_snapshot
    WHERE
        rn = 1
"""