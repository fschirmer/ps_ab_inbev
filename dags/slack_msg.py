# /opt/airflow/dags/simple_slack_inline_dag.py

from airflow.decorators import dag, task
from datetime import datetime, timezone
from airflow.hooks.base import BaseHook
import requests
import json

@dag(
    dag_id='simple_slack_inline_dag',
    start_date=datetime(2025, 7, 11, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=['slack', 'inline', 'hello'],
)
def simple_slack_inline_dag():

    # Função para obter a URL do webhook do Slack da conexão do Airflow
    def _get_slack_webhook_url():
        try:
            slack_connection = BaseHook.get_connection("slack_webhook")
            # Se o tipo de conexão for 'Slack', a URL completa estará no campo 'token'
            if hasattr(slack_connection, 'token') and slack_connection.token:
                return slack_connection.token
            # Se for 'Generic' ou 'HTTP', a URL é construída de host e password
            elif slack_connection.host and slack_connection.password:
                return f"{slack_connection.host}{slack_connection.password}"
            else:
                raise ValueError("Conexão Slack mal configurada. Verifique 'token' ou 'host'/'password'.")
        except Exception as e:
            print(f"Erro ao obter conexão Slack: {e}")
            raise

    @task
    def send_hello_to_slack_inline(**kwargs):
        """
        Tarefa que envia um 'Olá!' para o Slack usando uma função inline.
        """
        message_text = "Olá do seu Airflow! Esta é uma mensagem de teste INLINE na DAG."

        # Obter detalhes do contexto, se disponíveis
        dag_id = kwargs.get("dag").dag_id if kwargs.get("dag") else "N/A"
        task_id = kwargs.get("task_instance").task_id if kwargs.get("task_instance") else "N/A"
        execution_date = kwargs.get("execution_date") if kwargs.get("execution_date") else "N/A"

        full_message = f"{message_text}\n\n" \
                       f"_(DAG: {dag_id}, Task: {task_id}, Execução: {execution_date})_"

        print(f">> Tentando enviar mensagem ao Slack: {full_message}")

        try:
            webhook_url = _get_slack_webhook_url()

            payload = {
                "text": full_message,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": full_message
                        }
                    }
                ]
            }

            headers = {"Content-Type": "application/json"}
            response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)

            if response.status_code != 200:
                print(f"Erro ao enviar mensagem ao Slack: {response.text}")
                # Opcional: levante uma exceção para que a tarefa Airflow falhe
                # raise ValueError(f"Slack API retornou status {response.status_code}: {response.text}")
            else:
                print(f"Mensagem do Slack enviada com sucesso! Resposta: {response.status_code} - {response.text}")

        except Exception as e:
            print(f"Erro ao enviar mensagem para o Slack: {e}")
            raise # Re-raise a exceção para que a tarefa falhe no Airflow


    # Instanciação da tarefa
    send_hello_to_slack_inline_instance = send_hello_to_slack_inline()

# Instanciar o DAG
simple_slack_inline_dag()