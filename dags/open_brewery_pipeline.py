from airflow.decorators import dag, task
from datetime import datetime, timezone # Importe datetime e opcionalmente timezone

import os # Certifique-se de que 'os' está importado aqui
# Sua função de busca e salvamento da API
# (Coloque a função fetch_and_save_breweries_to_bronze aqui ou importe-a de um módulo Python separado dentro de 'plugins' ou 'dags')

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType
)
from pyspark.sql.functions import col, to_timestamp, lit, lower, regexp_replace, trim, when


# Defina o diretório de saída para a camada Bronze
BRONZE_LAYER_PATH = "/opt/airflow/data_lake/bronze"
SILVER_LAYER_PATH = "/opt/airflow/data_lake/silver"

@dag(
    dag_id='open_brewery_data_pipeline',
    start_date=datetime(2025, 7, 9, tzinfo=timezone.utc),
    schedule=None, # Rodar manualmente para teste ou defina seu agendamento
    catchup=False,
    tags=['brewery', 'data_lake', 'etl'],
)
def open_brewery_data_pipeline():

    @task
    def fetch_data_to_bronze():
        # Certifique-se de que a função fetch_and_save_breweries_to_bronze
        # está definida ou importada neste contexto.
        # Por exemplo, cole a função aqui ou em um arquivo utilitário importável.
        # ... (código da função fetch_and_save_breweries_to_bronze aqui) ...
        # (para fins de exemplo, vamos simular a função abaixo se ela estiver em outro arquivo)

        # Se a função estiver em um arquivo separado (ex: dags/utils/api_consumer.py):
        # from your_project.dags.utils.api_consumer import fetch_and_save_breweries_to_bronze

        # Para este exemplo, vou colar a função aqui para que você veja a estrutura completa
        import requests
        import json
        from datetime import datetime

        def _fetch_and_save_breweries_to_bronze(output_dir: str):
            base_url = "https://api.openbrewerydb.org/v1/breweries"
            all_breweries_data = []
            page = 1
            per_page = 200

            print(f"Iniciando a busca de dados da API: {base_url}")

            while True:
                params = {"page": page, "per_page": per_page}
                try:
                    response = requests.get(base_url, params=params)
                    response.raise_for_status()
                    breweries = response.json()

                    if not breweries:
                        print(f"Nenhum dado retornado na página {page}. Fim da paginação.")
                        break

                    all_breweries_data.extend(breweries)
                    print(f"Página {page} ({len(breweries)} cervejarias) buscada com sucesso.")
                    page += 1

                except requests.exceptions.RequestException as req_err:
                    print(f"Ocorreu um erro na requisição: {req_err}")
                    raise # Re-raise para que o Airflow marque a tarefa como falha

            if not all_breweries_data:
                print("Nenhum dado de cervejaria foi recuperado da API.")
                # Mude esta linha:
                # return
                # Para esta:
                raise ValueError(
                    "Nenhum dado de cervejaria foi recuperado da API para a camada Bronze.")

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"breweries_raw_{timestamp}.json"
            full_output_path = os.path.join(output_dir, file_name)

            os.makedirs(output_dir, exist_ok=True)

            try:
                with open(full_output_path, 'w', encoding='utf-8') as f:
                    for record in all_breweries_data:
                        f.write(json.dumps(record, ensure_ascii=False) + '\n')
                print(f"Dados brutos salvos com sucesso na camada Bronze: {full_output_path}")
                print(f"Total de {len(all_breweries_data)} registros salvos.")
                return full_output_path
            except IOError as e:
                print(f"Erro ao salvar o arquivo JSON: {e}")
                raise # Re-raise para que o Airflow marque a tarefa como falha

        return _fetch_and_save_breweries_to_bronze(BRONZE_LAYER_PATH)

    @task.pyspark(conn_id='spark_default')
    def transform_to_silver_task(bronze_file_path: str):
        """
        Lê dados da camada Bronze, transforma e salva na camada Silver em Parquet.
        """
        from pyspark.sql import SparkSession  # Importar SparkSession dentro da função PySpark

        # Criar uma sessão Spark
        spark = SparkSession.builder.appName("BrewerySilverLayer").getOrCreate()

        # 1. Definir o Schema Explícito para os dados da API (opcional, mas boa prática)
        # Este schema pode precisar de ajustes dependendo da sua observação dos dados brutos.
        brewery_schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("address_1", StringType(), True),
            StructField("address_2", StringType(), True),
            StructField("address_3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),  # Novo nome para o campo 'state'
            StructField("state", StringType(), True),  # O campo 'state' original pode ser útil
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("created_at", StringType(), True),  # Vem como string, será convertido
            StructField("updated_at", StringType(), True),  # Vem como string, será convertido
            StructField("street", StringType(), True)
            # Adicione mais campos conforme a estrutura da API
        ])

        # 2. Leitura dos Dados Brutos (Bronze)
        # Assumindo que os arquivos JSON estão na pasta BRONZE_LAYER_PATH
        # O Spark pode ler múltiplos arquivos JSON de uma pasta
        print(f"Lendo dados da camada Bronze do arquivo: {bronze_file_path}")
        df_bronze = spark.read.json(bronze_file_path,
                                    schema=brewery_schema)  # Ou .json(BRONZE_LAYER_PATH, multiLine=True) se o JSON não for JSON Lines

        print("Schema inferido/aplicado para a camada Bronze:")
        df_bronze.printSchema()
        print(f"Número de registros lidos da camada Bronze: {df_bronze.count()}")

        # 3. Transformações para a Camada Silver
        df_silver = df_bronze.select(
            col("id").alias("brewery_id"),  # Renomear para clareza
            col("name").alias("brewery_name"),
            col("brewery_type"),
            col("address_1").alias("address"),
            col("city"),
            col("state_province").alias("state_province_raw"),  # Manter o original
            col("state").alias("state_region"),
            # O Airflow 2.x Spark Provider usa `state` para o estado de TI. Renomear para evitar conflito.
            col("postal_code"),
            col("country"),
            col("longitude"),
            col("latitude"),
            col("phone"),
            col("website_url"),
            to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").alias("created_at_utc"),
            # Converter para Timestamp
            to_timestamp(col("updated_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ").alias("updated_at_utc")
            # Converter para Timestamp
        )

        # Transformações adicionais e criação de colunas para particionamento
        df_silver = df_silver.withColumn(
            "country_partition",
            # Padroniza para minúsculas e substitui espaços/caracteres especiais por underscore para nomes de pastas
            lower(regexp_replace(trim(col("country")), "[^a-z0-9_]", "_"))
        ).withColumn(
            "state_partition",
            # Padroniza para minúsculas e substitui espaços/caracteres especiais por underscore
            lower(regexp_replace(trim(col("state_region")), "[^a-z0-9_]", "_"))
        )
        # Tratar casos onde state_region pode ser nulo ou vazio e usar uma string padrão
        df_silver = df_silver.withColumn(
            "state_partition",
            when(col("state_partition").isNull() | (trim(col("state_partition")) == ""),
                 lit("unknown"))
            .otherwise(col("state_partition"))
        )
        # Tratar casos onde country pode ser nulo ou vazio e usar uma string padrão
        df_silver = df_silver.withColumn(
            "country_partition",
            when(col("country_partition").isNull() | (trim(col("country_partition")) == ""),
                 lit("unknown"))
            .otherwise(col("country_partition"))
        )

        print("Schema da camada Silver após transformações:")
        df_silver.printSchema()

        # 4. Persistência em Formato Parquet, particionado por país e estado
        # O modo 'overwrite' substitui a partição/pasta inteira se ela já existir
        print(f"Escrevendo dados transformados na camada Silver em: {SILVER_LAYER_PATH}")
        df_silver.write.mode("overwrite").partitionBy("country_partition",
                                                      "state_partition").parquet(SILVER_LAYER_PATH)
        print("Dados salvos com sucesso na camada Silver (Parquet, particionado).")
        spark.stop()  # Parar a sessão Spark

    bronze_output_path = fetch_data_to_bronze()
    transform_to_silver_task_instance = transform_to_silver_task(bronze_file_path=bronze_output_path)

# Instanciar o DAG
open_brewery_data_pipeline()