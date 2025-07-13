# /opt/airflow/dags/scripts/transform_to_gold.py

import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from metadata_utils import save_timestamp_to_metadata_file, read_timestamp_from_metadata_file
from queries_gold_breweries import GOLD_TRANSFORMATION_SQL

SILVER_LAYER_PATH = "/opt/airflow/data_lake/silver"
GOLD_LAYER_PATH = "/opt/airflow/data_lake/gold"

SILVER_TABLE_ROOT_PATH = os.path.join(SILVER_LAYER_PATH, "breweries", "data")
SILVER_METADATA_FILE_PATH = os.path.join(
    SILVER_LAYER_PATH, "breweries", "metadata", "last_processed_timestamp.txt"
)

GOLD_TABLE_ROOT_PATH = os.path.join(GOLD_LAYER_PATH, "breweries", "data")
GOLD_TABLE_ROOT_PATH2 = os.path.join(GOLD_LAYER_PATH, "breweries", "data_csv")

GOLD_METADATA_FILE_PATH = os.path.join(
    GOLD_LAYER_PATH, "breweries", "metadata", "last_processed_timestamp.txt"
)


def run_transform_to_gold():
    """
    Função principal para executar a transformação Spark da camada Silver para Gold.
    Lê dados da Silver (Delta), agrega e salva em Delta no Gold.
    A Gold agora garante que apenas os registros mais atuais da Silver sejam agregados.
    """
    spark = SparkSession.builder \
        .appName("BreweryGoldAggregation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print(f"Lendo o último timestamp processado da camada Gold em: {GOLD_METADATA_FILE_PATH}")
    last_processed_gold_timestamp = read_timestamp_from_metadata_file(GOLD_METADATA_FILE_PATH)

    start_timestamp_for_silver_read = datetime(1970, 1, 1, tzinfo=timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f")
    if last_processed_gold_timestamp:
        start_timestamp_for_silver_read = last_processed_gold_timestamp

    print(
        f"A camada Gold agregará o estado atual da Silver. Último timestamp processado: {start_timestamp_for_silver_read}")

    try:
        df_silver = spark.read.format("delta").load(SILVER_TABLE_ROOT_PATH)
        print(f"Dados lidos com sucesso da camada Silver (Delta): {SILVER_TABLE_ROOT_PATH}")

        df_silver.createOrReplaceTempView("silver_breweries")
        print("DataFrame 'silver_breweries' registrado como view temporária.")

        initial_silver_count = df_silver.count()
        if initial_silver_count == 0:
            print("A camada Silver está vazia. Nenhum dado para processar para a Gold.")
            spark.stop()
            return

        sql_query_gold = f"""
            SELECT
                country AS country, -- Usando 'country' diretamente como nome da coluna
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

        sql_query_gold = GOLD_TRANSFORMATION_SQL
        df_gold = spark.sql(sql_query_gold)

        if df_gold.count() == 0:
            print("Nenhum registro ativo na camada Silver para processar para a Gold após a filtragem.")
            spark.stop()
            return

        print("Schema da camada Gold agregada (via SQL):")
        df_gold.printSchema()
        df_gold.show(5)

    except Exception as e:
        print(f"Erro ao ler ou processar os dados da camada Silver/Gold com SQL: {e}")
        spark.stop()
        raise

    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_TABLE_ROOT_PATH)

    print(f"Executando OPTIMIZE ZORDER BY (country, brewery_type) na tabela Delta Gold: {GOLD_TABLE_ROOT_PATH}")
    spark.sql(f"OPTIMIZE delta.`{GOLD_TABLE_ROOT_PATH}` ZORDER BY (country, brewery_type)")
    print("OPTIMIZE com ZORDER BY (country, brewery_type) concluído.")

    df_gold.write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save(GOLD_TABLE_ROOT_PATH2)

    print(f"Dados agregados salvos com sucesso na camada Gold (Delta): {GOLD_TABLE_ROOT_PATH}")
    print(f"Dados agregados salvos em formato CSV em: {GOLD_TABLE_ROOT_PATH2}")

    sql_query_max_timestamp = f"""
        SELECT
            MAX(bronze_file_timestamp)
        FROM
            silver_breweries
    """
    latest_bronze_timestamp_processed_for_gold = spark.sql(sql_query_max_timestamp).collect()[0][0]

    if latest_bronze_timestamp_processed_for_gold:
        save_timestamp_to_metadata_file(latest_bronze_timestamp_processed_for_gold, GOLD_METADATA_FILE_PATH)
    else:
        print("Aviso: Nenhum timestamp Bronze ativo processado para atualizar o metadado Gold.")

    spark.stop()


if __name__ == "__main__":
    run_transform_to_gold()
