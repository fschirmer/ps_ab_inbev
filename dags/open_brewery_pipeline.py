from airflow.decorators import dag, task
from datetime import datetime, timezone # Importe datetime e opcionalmente timezone
# from utils.text_processing import to_ascii_safe_spark_udf
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
GOLD_LAYER_PATH = "/opt/airflow/data_lake/gold"

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

                    # --- ADICIONE ESTE BLOCO DE DEBUG AQUI ---
                    for brewery in breweries:
                        if brewery.get('country') == 'Austria' and 'state_province' in brewery:
                            print(f"DEBUG: Encontrado registro da Áustria. state_province: '{brewery['state_province']}' (tipo: {type(brewery['state_province'])})")
                            # Se você quiser inspecionar os bytes brutos (avançado):
                            # print(f"DEBUG: Bytes de state_province: {brewery['state_province'].encode('utf-8')}")
                    # --- FIM DO BLOCO DE DEBUG ---

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

    bronze_output_path = fetch_data_to_bronze()

    transform_to_silver_task = SparkSubmitOperator(
        task_id='transform_to_silver_task',
        conn_id='spark_default',  # Ou sua conexão dedicada se preferir
        application='/opt/airflow/dags/scripts/transform_data.py',
        # O script PySpark a ser executado

        # Onde você passa o seu módulo utils:
        py_files='/opt/airflow/dags/utils/text_processing.py',  # Caminho para o seu zip de utilitários
        # Certifique-se de que utils.zip existe em dags/
        # Ou use '/opt/airflow/dags/utils/text_processing.py'
        # se for apenas um arquivo .py sem ser um pacote zipado.

        # Argumentos para o seu script PySpark: o caminho do arquivo bronze
        application_args=[bronze_output_path],

        # Opcional: Configurações Spark adicionais, se necessário
        # conf={'spark.driver.memory': '2g', 'spark.executor.memory': '4g'},

        # Se você está usando Pandas UDFs, o ambiente Spark deve ter pandas e pyarrow instalados.
        # Isso é gerenciado pelo Dockerfile.Spark, não por aqui.
    )

    transform_to_gold_task = SparkSubmitOperator(
        task_id='transform_to_gold',
        application='/opt/airflow/dags/scripts/transform_to_gold.py', # Caminho para o seu script Spark
        conn_id='spark_default',
        # Configurações adicionais do Spark, se necessário
        conf={"spark.driver.maxResultSize": "4g"},
        # Argumentos passados para o script Spark
        application_args=[
            '--silver_layer_path', SILVER_LAYER_PATH, # Caminho do diretório da camada Silver
            '--gold_layer_path', GOLD_LAYER_PATH
        ],
    )

    # transform_to_silver_task_instance = transform_to_silver_task(bronze_file_path=bronze_output_path)
    bronze_output_path >> transform_to_silver_task >> transform_to_gold_task

# Instanciar o DAG
open_brewery_data_pipeline()