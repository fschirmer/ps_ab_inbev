# /opt/airflow/dags/scripts/transform_data.py

import sys
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, input_file_name

script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

from text_processing import to_ascii_safe_python, to_ascii_safe_spark_udf
from metadata_utils import save_timestamp_to_metadata_file, read_timestamp_from_metadata_file
from queries_silver_breweries import SILVER_TRANSFORMATION_SQL

BRONZE_LAYER_PATH = "/opt/airflow/data_lake/bronze"
SILVER_LAYER_PATH = "/opt/airflow/data_lake/silver"

BRONZE_METADATA_FILE_PATH = os.path.join(
    BRONZE_LAYER_PATH, "breweries", "metadata", "last_processed_timestamp.txt"
)
SILVER_METADATA_FILE_PATH = os.path.join(
    SILVER_LAYER_PATH, "breweries", "metadata", "last_processed_timestamp.txt"
)

def run_transform_to_silver():
    """
    Função principal para executar a transformação Spark da camada Bronze para Silver.
    Lê dados incrementalmente da Bronze, transforma e salva em Delta no Silver usando OVERWRITE.
    """
    spark = SparkSession.builder \
        .appName("BreweryDataTransformToSilver") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    last_processed_silver_timestamp = read_timestamp_from_metadata_file(SILVER_METADATA_FILE_PATH)

    start_timestamp_for_bronze_read = datetime(1970, 1, 1, tzinfo=timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f")
    if last_processed_silver_timestamp:
        start_timestamp_for_bronze_read = last_processed_silver_timestamp

    print(
        f"Iniciando leitura da camada Bronze a partir do timestamp: {start_timestamp_for_bronze_read}")

    bronze_base_data_path = os.path.join(BRONZE_LAYER_PATH, "breweries", "data")
    silver_output_table_path = os.path.join(SILVER_LAYER_PATH, "breweries", "data")
    silver_output_csv_path = os.path.join(SILVER_LAYER_PATH, "breweries", "csv_data")

    try:
        df_bronze_raw = spark.read.json(os.path.join(bronze_base_data_path, "*/*/*/*.json"))

        df_bronze_with_timestamp = df_bronze_raw.withColumn(
            "bronze_file_timestamp",
            regexp_extract(input_file_name(), r'breweries_(\d{8}_\d{6}_\d{6})\.json$', 1)
        )

        df_bronze_with_timestamp.createOrReplaceTempView("bronze_raw_view")
        print("DataFrame 'bronze_raw_view' registrado como view temporária.")

        spark.udf.register("to_ascii_safe_udf", to_ascii_safe_spark_udf)
        print("UDF 'to_ascii_safe_udf' registrada para uso em SQL.")

        sql_query_silver = SILVER_TRANSFORMATION_SQL.format(
            start_timestamp_for_bronze_read=start_timestamp_for_bronze_read
        )
        df_silver = spark.sql(sql_query_silver)

        print("------#######################################------------")

        geonames_csv_path = "/opt/airflow/data/geonames-postal-code.csv"

        geonames_df = spark.read.csv(
            geonames_csv_path,
            header=True,
            inferSchema=True,
            sep=',',
            encoding='UTF-8'
        )

        print(sql_query_silver)
        df_isle_of_man = df_silver.filter("country = 'austria'")
        df_isle_of_man.select("id", "country", "bronze_file_timestamp").show(truncate=False)
        print("------#######################################------------")

        if df_silver.count() == 0:
            print(
                "Nenhum novo dado na camada Bronze para processar para a Silver após a filtragem SQL.")
            spark.stop()
            return

        print("Schema da camada Silver (via SQL):")
        df_silver.printSchema()
        df_silver.show(5)

        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("country") \
            .save(silver_output_table_path)
        print(f"Dados Silver salvos em formato Delta (OVERWRITE) em: {silver_output_table_path}")

        df_silver.write \
            .format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .partitionBy("country") \
            .save(silver_output_csv_path)
        print(f"Dados Silver (estado atual) salvos em formato CSV em: {silver_output_csv_path}")

    except Exception as e:
        print(f"Erro no processo de transformação para Silver: {e}")
        spark.stop()
        raise

    sql_query_max_timestamp = f"""
        SELECT
            MAX(bronze_file_timestamp)
        FROM
            bronze_raw_view
        WHERE
            bronze_file_timestamp > '{start_timestamp_for_bronze_read}'
    """
    latest_bronze_timestamp_processed = spark.sql(sql_query_max_timestamp).collect()[0][0]

    if latest_bronze_timestamp_processed:
        save_timestamp_to_metadata_file(latest_bronze_timestamp_processed,
                                        SILVER_METADATA_FILE_PATH)
    else:
        print("Aviso: Nenhum novo timestamp Bronze processado para atualizar o metadado Silver.")

    spark.stop()


if __name__ == "__main__":
    run_transform_to_silver()
