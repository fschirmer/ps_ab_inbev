# /opt/airflow/dags/scripts/transform_to_gold.py

import sys
import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp # Mantemos current_timestamp para as colunas created_at/updated_at

# --- Definição de Caminhos ---
# Estes são os caminhos base para as camadas no Data Lake.
# Os caminhos completos para as tabelas Delta e metadados serão construídos a partir daqui.
SILVER_LAYER_PATH = "/opt/airflow/data_lake/silver"
GOLD_LAYER_PATH = "/opt/airflow/data_lake/gold"

# Caminho para a raiz da tabela Delta da camada Silver
SILVER_TABLE_ROOT_PATH = os.path.join(SILVER_LAYER_PATH, "breweries", "data")
# Caminho para a pasta de metadados da camada Silver (para ler o último timestamp processado)
SILVER_METADATA_FILE_PATH = os.path.join(
    SILVER_LAYER_PATH, "breweries", "metadata", "last_processed_timestamp.txt"
)

# Caminho para a raiz da tabela Delta da camada Gold
GOLD_TABLE_ROOT_PATH = os.path.join(GOLD_LAYER_PATH, "breweries", "data")
GOLD_TABLE_ROOT_PATH2 = os.path.join(GOLD_LAYER_PATH, "breweries", "data_json")
# Caminho para a pasta de metadados da camada Gold (para gravar o último timestamp processado)
GOLD_METADATA_FILE_PATH = os.path.join(
    GOLD_LAYER_PATH, "breweries", "metadata", "last_processed_timestamp.txt"
)


def save_timestamp_to_metadata_file(timestamp_str: str, file_path: str):
    """
    Salva um timestamp (como string) em um arquivo de metadados.

    Cria o diretório pai do arquivo se ele não existir.

    Args:
        timestamp_str (str): O timestamp a ser salvo (ex: "20250711_221343_092307").
        file_path (str): O caminho completo para o arquivo de metadados.

    Returns:
        bool: True se o timestamp foi salvo com sucesso, False caso contrário.
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

    Args:
        file_path (str): O caminho completo para o arquivo de metadados.

    Returns:
        str | None: O timestamp lido como string, ou None se o arquivo não existir.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            timestamp = f.read().strip()
            print(f"Timestamp lido de '{file_path}': '{timestamp}'.")
            return timestamp
    except FileNotFoundError:
        print(f"Arquivo de metadados '{file_path}' não encontrado. Assumindo primeiro processamento.")
        return None
    except IOError as e:
        print(f"Erro ao ler o timestamp do arquivo '{file_path}': {e}")
        return None

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

    # Define um timestamp inicial muito antigo se for a primeira execução da Gold
    # Este timestamp é para fins de metadado.
    start_timestamp_for_silver_read = datetime(1970, 1, 1, tzinfo=timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f")
    if last_processed_gold_timestamp:
        start_timestamp_for_silver_read = last_processed_gold_timestamp

    print(
        f"A camada Gold agregará o estado atual da Silver. Último timestamp processado: {start_timestamp_for_silver_read}")

    try:
        # 1. Leitura dos dados da camada Silver (agora em formato Delta)
        # O caminho é a raiz da tabela Delta.
        df_silver = spark.read.format("delta").load(SILVER_TABLE_ROOT_PATH)
        print(f"Dados lidos com sucesso da camada Silver (Delta): {SILVER_TABLE_ROOT_PATH}")

        # Registrar o DataFrame da camada Silver como uma view temporária para consultas SQL
        df_silver.createOrReplaceTempView("silver_breweries")
        print("DataFrame 'silver_breweries' registrado como view temporária.")

        # Contar o número de linhas para verificar se há dados antes de filtrar
        initial_silver_count = df_silver.count()
        if initial_silver_count == 0:
            print("A camada Silver está vazia. Nenhum dado para processar para a Gold.")
            spark.stop()
            return # Sai da função se não houver dados

        # 2. Agregação dos dados usando Spark SQL: quantidade de cervejarias por tipo e país
        # Reintroduzindo a lógica de ROW_NUMBER() para garantir que apenas a última versão
        # de cada ID seja considerada, já que a Silver agora usa overwrite e não tem 'is_deleted'.
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
        df_gold = spark.sql(sql_query_gold)

        if df_gold.count() == 0:
            print("Nenhum registro ativo na camada Silver para processar para a Gold após a filtragem.")
            spark.stop()
            return  # Sai da função se não houver novos dados

        print("Schema da camada Gold agregada (via SQL):")
        df_gold.printSchema()
        df_gold.show(5)

    except Exception as e:
        print(f"Erro ao ler ou processar os dados da camada Silver/Gold com SQL: {e}")
        spark.stop()
        raise  # Re-raise para que o Airflow marque a tarefa como falha

    # 3. Escrita dos dados agregados na camada Gold (em formato Delta)
    # O caminho é a raiz da tabela Delta da Gold
    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_TABLE_ROOT_PATH)

    # --- Adição do OPTIMIZE com ZORDER BY na camada Gold ---
    # Aplica ZORDER BY nas colunas de agrupamento para otimizar futuras leituras
    print(f"Executando OPTIMIZE ZORDER BY (country, brewery_type) na tabela Delta Gold: {GOLD_TABLE_ROOT_PATH}")
    spark.sql(f"OPTIMIZE delta.`{GOLD_TABLE_ROOT_PATH}` ZORDER BY (country, brewery_type)")
    print("OPTIMIZE com ZORDER BY (country, brewery_type) concluído.")

    # CSV
    # Salva o resultado da AGREGAÇÃO da Gold em CSV, sobrescrevendo.
    df_gold.write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save(GOLD_TABLE_ROOT_PATH2)

    print(f"Dados agregados salvos com sucesso na camada Gold (Delta): {GOLD_TABLE_ROOT_PATH}")
    print(f"Dados agregados salvos em formato CSV em: {GOLD_TABLE_ROOT_PATH2}")

    # --- 4. Atualizar o arquivo de metadados da camada Gold ---
    # Para obter o maior timestamp bronze processado, vamos consultar a view temporária
    # da Silver, considerando apenas os registros mais recentes (rn=1).
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


# Este bloco garante que a função principal seja chamada quando o script for executado via spark-submit
if __name__ == "__main__":
    run_transform_to_gold()
