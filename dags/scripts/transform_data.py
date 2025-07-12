# /opt/airflow/dags/scripts/transform_data.py

import sys
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, input_file_name, current_timestamp, lit
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, BooleanType, \
    TimestampType
# from delta.tables import DeltaTable  # Removido: Não é mais necessário para MERGE

# Importe sua UDF de text_processing
try:
    from text_processing import to_ascii_safe
except ImportError:
    print(
        "Aviso: 'text_processing.py' ou a função 'to_ascii_safe' não encontrada. Usando placeholder.")


    def to_ascii_safe(text):
        if text is None:
            return None
        return ''.join(char for char in text if ord(char) < 128).lower().replace(" ", "_")

# --- Definição de Caminhos ---
BRONZE_LAYER_PATH = "/opt/airflow/data_lake/bronze"
SILVER_LAYER_PATH = "/opt/airflow/data_lake/silver"

BRONZE_METADATA_FILE_PATH = os.path.join(
    BRONZE_LAYER_PATH, "breweries", "metadata", "last_processed_timestamp.txt"
)
SILVER_METADATA_FILE_PATH = os.path.join(
    SILVER_LAYER_PATH, "breweries", "metadata", "last_processed_timestamp.txt"
)


def save_timestamp_to_metadata_file(timestamp_str: str, file_path: str):
    """
    Salva um timestamp (como string) em um arquivo de metadados.
    Cria o diretório pai do arquivo se ele não existir.
    """
    dir_name = os.path.dirname(file_path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
        print(f"Diretório '{dir_name}' garantido/criado para metadados.")

    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(timestamp_str)
        print(f"Timestamp '{timestamp_str}' salvo com sucesso em '{file_path}'.")
        return True
    except IOError as e:
        print(f"Erro ao salvar o timestamp no arquivo '{file_path}': {e}")
        return False


def read_timestamp_from_metadata_file(file_path: str) -> str | None:
    """
    Lê um timestamp de um arquivo de metadados.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            timestamp = f.read().strip()
            print(f"Timestamp lido de '{file_path}': '{timestamp}'.")
            return timestamp
    except FileNotFoundError:
        print(
            f"Arquivo de metadados '{file_path}' não encontrado. Assumindo primeiro processamento.")
        return None
    except IOError as e:
        print(f"Erro ao ler o timestamp do arquivo '{file_path}': {e}")
        return None


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

        spark.udf.register("to_ascii_safe_udf", to_ascii_safe, StringType())
        print("UDF 'to_ascii_safe_udf' registrada para uso em SQL.")

        # --- 3. Transformação e Limpeza usando Spark SQL ---
        # Garante que apenas a última versão de cada ID seja selecionada do lote incremental da Bronze.
        sql_query_silver = f"""
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
        df_silver = spark.sql(sql_query_silver) # Renomeado de df_silver_source para df_silver

        if df_silver.count() == 0:
            print(
                "Nenhum novo dado na camada Bronze para processar para a Silver após a filtragem SQL.")
            spark.stop()
            return

        print("Schema da camada Silver (via SQL):")
        df_silver.printSchema()
        df_silver.show(5)

        # --- 4. Gravar em Delta (modo OVERWRITE) ---
        # Removida a lógica de MERGE INTO e verificação DeltaTable.isDeltaTable
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("country") \
            .save(silver_output_table_path)
        print(f"Dados Silver salvos em formato Delta (OVERWRITE) em: {silver_output_table_path}")

        # Removido o OPTIMIZE ZORDER BY (id) conforme solicitação de retorno ao overwrite original.

        # Salvar também em formato CSV (opcional, para depuração ou outros usos)
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

    # --- 5. Atualizar o arquivo de metadados da camada Silver ---
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


# Este bloco verifica se o script está sendo executado diretamente pelo spark-submit
if __name__ == "__main__":
    run_transform_to_silver()
