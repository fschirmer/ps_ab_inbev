GOLD_TRANSFORMATION_SQL = """
    SELECT
        country,
        brewery_type,
        COUNT(id) AS quantity_of_breweries,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    FROM
        silver_breweries
    GROUP BY
        country,
        brewery_type
"""