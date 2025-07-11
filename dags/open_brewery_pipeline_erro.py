from airflow.decorators import dag, task
from datetime import datetime, timezone, timedelta # Importe datetime e opcionalmente timezone
# from utils.text_processing import to_ascii_safe_spark_udf
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os # Certifique-se de que 'os' está importado aqui
# Sua função de busca e salvamento da API
# (Coloque a função fetch_and_save_breweries_to_bronze aqui ou importe-a de um módulo Python separado dentro de 'plugins' ou 'dags')
import pendulum
from urllib.parse import quote
from alerts import send_slack_alert

# # --- INÍCIO DA FUNÇÃO send_slack_alert A SER COLADA AQUI ---
# from airflow.hooks.base import BaseHook
# import requests
# import json
#
# def _get_slack_webhook_url():
#     """Helper para obter a URL do webhook do Slack da conexão do Airflow."""
#     try:
#         slack_connection = BaseHook.get_connection("slack_webhook")
#         # Se o tipo de conexão for 'Slack', a URL completa estará no campo 'token'
#         if hasattr(slack_connection, 'token') and slack_connection.token:
#             return slack_connection.token
#         # Se for 'Generic' ou 'HTTP', a URL é construída de host e password
#         elif slack_connection.host and slack_connection.password:
#             # Garante que não haja barras duplas se host já terminar em barra e password começar com barra
#             if not slack_connection.host.endswith('/') and not slack_connection.password.startswith(
#                     '/'):
#                 return f"{slack_connection.host}/{slack_connection.password}"
#             else:
#                 return f"{slack_connection.host}{slack_connection.password}"
#         else:
#             raise ValueError(
#                 "Conexão Slack mal configurada. Verifique 'token' ou 'host'/'password'.")
#     except Exception as e:
#         # Imprime o erro para os logs do scheduler
#         print(f"Erro ao obter conexão Slack: {e}")
#         # É importante levantar a exceção para que o Airflow saiba que o callback falhou
#         raise
#
# from urllib.parse import quote
# # Não precisamos mais de pendulum ou datetime para esta estrutura de URL,
# # mas se você os usa em outro lugar, pode mantê-los.
# # import pendulum
# # from datetime import datetime
#
# def send_alert_with_log_url(context):
#     dag_id = context.get("dag").dag_id
#     task_id = context.get("task_instance").task_id
#     run_id = context.get("run_id")
#     # O map_index não é usado diretamente nesta URL de detalhes da tarefa
#     # logical_date também não é necessário para esta URL de detalhes da tarefa
#
#     # IMPORTANTE: Use a URL base do seu Airflow Webserver
#     airflow_webserver_base_url = "http://localhost:8080" # Ou a sua URL real
#
#     # Codifica os componentes da URL
#     quoted_dag_id = quote(dag_id)
#     quoted_task_id = quote(task_id)
#     quoted_run_id = quote(run_id)
#
#     # Constrói a URL para a página de detalhes da instância da tarefa
#     # Esta é a URL que você obteve do navegador!
#     log_url = (
#         f"{airflow_webserver_base_url}/dags/"
#         f"{quoted_dag_id}/runs/"
#         f"{quoted_run_id}/tasks/"
#         f"{quoted_task_id}"
#     )
#
#     print(f"URL da Página de Detalhes da Tarefa Gerada: {log_url}")
#     return log_url
#
# def send_slack_alert(context):
#     """
#     Função de callback que envia um alerta detalhado para o Slack.
#     Ajustada para extrair informações mais robustas em callbacks de DAG.
#     """
#     print(">>> send_slack_alert INVOCADA (com lógica de Slack)! <<<")
#
#     try:
#         webhook_url = _get_slack_webhook_url()
#         print(f">> Webhook URL obtida (parcialmente oculta por segurança): {webhook_url[:40]}...")
#
#         dag_id = context.get("dag").dag_id if context.get("dag") else "N/A"
#         execution_date = context.get("execution_date") if context.get("execution_date") else "N/A"
#
#         # Inicializa variáveis para informações específicas da tarefa
#         task_id = "N/A"
#         log_url = "N/A"
#         reason = "Motivo desconhecido (erro geral da DAG)"
#
#         # Tenta obter a instância da tarefa diretamente do contexto
#         ti = context.get("task_instance")
#         if ti:  # Se a instância da tarefa estiver diretamente disponível (comum em callbacks de tarefa)
#             task_id = ti.task_id
#             # log_url = ti.get_log_url()
#             log_url = send_alert_with_log_url(context)
#             if hasattr(ti, 'exception') and ti.exception:
#                 reason = str(ti.exception)
#             elif context.get("exception"):  # Fallback para a exceção geral do contexto
#                 reason = str(context.get("exception"))
#         else:
#             # Se task_instance não estiver diretamente no contexto, procura na dag_run
#             dag_run = context.get("dag_run")
#             if dag_run:
#                 # Encontra todas as instâncias de tarefa que falharam nesta execução da DAG
#                 failed_tis = [
#                     t_i for t_i in dag_run.get_task_instances() if t_i.current_state() == "failed"
#                 ]
#                 if failed_tis:
#                     # Pega a primeira tarefa que falhou (poderia haver múltiplas)
#                     ti = failed_tis[0]
#                     task_id = ti.task_id
#                     # log_url = ti.get_log_url()
#                     log_url = send_alert_with_log_url(context)
#                     if hasattr(ti, 'exception') and ti.exception:
#                         reason = str(ti.exception)
#                     elif ti.log_template:  # Às vezes a exceção está no log_template para falha de tarefa
#                         # Podemos tentar extrair mais detalhes daqui se necessário
#                         pass
#
#                         # Se nenhuma instância de tarefa falha específica foi encontrada, usa a exceção geral da DAG
#                 if not ti and context.get("exception"):
#                     reason = str(context.get("exception"))
#
#         message_text = f":red_circle: *FALHA NA DAG do Airflow!* :red_circle:\n\n" \
#                        f"*DAG ID:* `{dag_id}`\n" \
#                        f"*Task ID:* `{task_id}`\n" \
#                        f"*Execução:* `{execution_date}`\n" \
#                        f"*URL do Log:* <{log_url}|Ver Logs>\n" \
#                        f"*Motivo da Falha:* ```{reason}```"
#
#         payload = {
#             "text": message_text,
#             "blocks": [
#                 {
#                     "type": "section",
#                     "text": {
#                         "type": "mrkdwn",
#                         "text": message_text
#                     }
#                 }
#             ]
#         }
#
#         headers = {"Content-Type": "application/json"}
#         response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)
#
#         if response.status_code != 200:
#             print(
#                 f"Erro ao enviar mensagem ao Slack. Status: {response.status_code}, Resposta: {response.text}")
#         else:
#             print(
#                 f"Mensagem do Slack enviada com sucesso! Status: {response.status_code} - {response.text}")
#
#     except Exception as e:
#         print(f"Erro CRÍTICO ao tentar enviar alerta para o Slack: {e}")
#         raise  # Re-raise a exceção para que o Airflow saiba que o callback falhou
#
#
# # --- FIM: FUNÇÕES DE CALLBACK DO SLACK ---

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DoubleType
)
from pyspark.sql.functions import col, to_timestamp, lit, lower, regexp_replace, trim, when


# Defina o diretório de saída para a camada Bronze
BRONZE_LAYER_PATH = "/opt/airflow/data_lake/bronze"
SILVER_LAYER_PATH = "/opt/airflow/data_lake/silver"
GOLD_LAYER_PATH = "/opt/airflow/data_lake/gold"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False, # Desativar e-mail se você usa Slack
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0),
    'on_failure_callback': send_slack_alert, # AQUI! No default_args
}

@dag(
    dag_id='open_brewery_data_pipeline_erro',
    start_date=datetime(2025, 7, 9, tzinfo=timezone.utc),
    schedule=None, # Rodar manualmente para teste ou defina seu agendamento
    catchup=False,
    tags=['brewery', 'data_lake', 'etl'],
    default_args=default_args,
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

            resultado = 1 / 0
            print(resultado)

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