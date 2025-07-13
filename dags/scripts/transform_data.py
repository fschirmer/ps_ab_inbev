# /opt/airflow/dags/scripts/transform_data.py

import sys
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, input_file_name, current_timestamp, lit
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, BooleanType, \
    TimestampType
# from delta.tables import DeltaTable  # Removido: Não é mais necessário para MERGE


# from utils.text_processing import to_ascii_safe_python, to_ascii_safe_spark_udf
# from queries.silver.breweries.queries_silver_breweries import SILVER_TRANSFORMATION_SQL

# --- ADICIONE ESTAS LINHAS AQUI ---
# Adiciona o diretório do script atual ao sys.path.
# O Spark copia todos os py_files para o mesmo diretório onde o application é executado.
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir) # Use insert(0, ...) para dar prioridade
# --- FIM DAS LINHAS A SEREM ADICIONADAS ---

from text_processing import to_ascii_safe_python, to_ascii_safe_spark_udf
from queries_silver_breweries import SILVER_TRANSFORMATION_SQL

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

        spark.udf.register("to_ascii_safe_udf", to_ascii_safe_spark_udf)
        print("UDF 'to_ascii_safe_udf' registrada para uso em SQL.")

        # --- 3. Transformação e Limpeza usando Spark SQL ---
        # Garante que apenas a última versão de cada ID seja selecionada do lote incremental da Bronze.
        sql_query_silver = SILVER_TRANSFORMATION_SQL.format(
            start_timestamp_for_bronze_read=start_timestamp_for_bronze_read
        )
        df_silver = spark.sql(sql_query_silver) # Renomeado de df_silver_source para df_silver
        print("------#######################################------------")
        print(sql_query_silver)
        df_isle_of_man = df_silver.filter("country = 'isle of man'")
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
