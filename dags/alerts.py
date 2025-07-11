from airflow.hooks.base import BaseHook
import requests
import json

def _get_slack_webhook_url():
    """Helper para obter a URL do webhook do Slack da conexão do Airflow."""
    try:
        slack_connection = BaseHook.get_connection("slack_webhook")
        # Se o tipo de conexão for 'Slack', a URL completa estará no campo 'token'
        if hasattr(slack_connection, 'token') and slack_connection.token:
            return slack_connection.token
        # Se for 'Generic' ou 'HTTP', a URL é construída de host e password
        elif slack_connection.host and slack_connection.password:
            # Garante que não haja barras duplas se host já terminar em barra e password começar com barra
            if not slack_connection.host.endswith('/') and not slack_connection.password.startswith(
                    '/'):
                return f"{slack_connection.host}/{slack_connection.password}"
            else:
                return f"{slack_connection.host}{slack_connection.password}"
        else:
            raise ValueError(
                "Conexão Slack mal configurada. Verifique 'token' ou 'host'/'password'.")
    except Exception as e:
        # Imprime o erro para os logs do scheduler
        print(f"Erro ao obter conexão Slack: {e}")
        # É importante levantar a exceção para que o Airflow saiba que o callback falhou
        raise

from urllib.parse import quote
# Não precisamos mais de pendulum ou datetime para esta estrutura de URL,
# mas se você os usa em outro lugar, pode mantê-los.
# import pendulum
# from datetime import datetime

def send_alert_with_log_url(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    run_id = context.get("run_id")
    # O map_index não é usado diretamente nesta URL de detalhes da tarefa
    # logical_date também não é necessário para esta URL de detalhes da tarefa

    # IMPORTANTE: Use a URL base do seu Airflow Webserver
    airflow_webserver_base_url = "http://localhost:8080" # Ou a sua URL real

    # Codifica os componentes da URL
    quoted_dag_id = quote(dag_id)
    quoted_task_id = quote(task_id)
    quoted_run_id = quote(run_id)

    # Constrói a URL para a página de detalhes da instância da tarefa
    # Esta é a URL que você obteve do navegador!
    log_url = (
        f"{airflow_webserver_base_url}/dags/"
        f"{quoted_dag_id}/runs/"
        f"{quoted_run_id}/tasks/"
        f"{quoted_task_id}"
    )

    print(f"URL da Página de Detalhes da Tarefa Gerada: {log_url}")
    return log_url

def send_slack_alert(context):
    """
    Função de callback que envia um alerta detalhado para o Slack.
    Ajustada para extrair informações mais robustas em callbacks de DAG.
    """
    print(">>> send_slack_alert INVOCADA (com lógica de Slack)! <<<")

    try:
        webhook_url = _get_slack_webhook_url()
        print(f">> Webhook URL obtida (parcialmente oculta por segurança): {webhook_url[:40]}...")

        dag_id = context.get("dag").dag_id if context.get("dag") else "N/A"
        execution_date = context.get("execution_date") if context.get("execution_date") else "N/A"

        # Inicializa variáveis para informações específicas da tarefa
        task_id = "N/A"
        log_url = "N/A"
        reason = "Motivo desconhecido (erro geral da DAG)"

        # Tenta obter a instância da tarefa diretamente do contexto
        ti = context.get("task_instance")
        if ti:  # Se a instância da tarefa estiver diretamente disponível (comum em callbacks de tarefa)
            task_id = ti.task_id
            # log_url = ti.get_log_url()
            log_url = send_alert_with_log_url(context)
            if hasattr(ti, 'exception') and ti.exception:
                reason = str(ti.exception)
            elif context.get("exception"):  # Fallback para a exceção geral do contexto
                reason = str(context.get("exception"))
        else:
            # Se task_instance não estiver diretamente no contexto, procura na dag_run
            dag_run = context.get("dag_run")
            if dag_run:
                # Encontra todas as instâncias de tarefa que falharam nesta execução da DAG
                failed_tis = [
                    t_i for t_i in dag_run.get_task_instances() if t_i.current_state() == "failed"
                ]
                if failed_tis:
                    # Pega a primeira tarefa que falhou (poderia haver múltiplas)
                    ti = failed_tis[0]
                    task_id = ti.task_id
                    # log_url = ti.get_log_url()
                    log_url = send_alert_with_log_url(context)
                    if hasattr(ti, 'exception') and ti.exception:
                        reason = str(ti.exception)
                    elif ti.log_template:  # Às vezes a exceção está no log_template para falha de tarefa
                        # Podemos tentar extrair mais detalhes daqui se necessário
                        pass

                        # Se nenhuma instância de tarefa falha específica foi encontrada, usa a exceção geral da DAG
                if not ti and context.get("exception"):
                    reason = str(context.get("exception"))

        message_text = f":red_circle: *FALHA NA DAG do Airflow!* :red_circle:\n\n" \
                       f"*DAG ID:* `{dag_id}`\n" \
                       f"*Task ID:* `{task_id}`\n" \
                       f"*Execução:* `{execution_date}`\n" \
                       f"*URL do Log:* <{log_url}|Ver Logs>\n" \
                       f"*Motivo da Falha:* ```{reason}```"

        payload = {
            "text": message_text,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": message_text
                    }
                }
            ]
        }

        headers = {"Content-Type": "application/json"}
        response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)

        if response.status_code != 200:
            print(
                f"Erro ao enviar mensagem ao Slack. Status: {response.status_code}, Resposta: {response.text}")
        else:
            print(
                f"Mensagem do Slack enviada com sucesso! Status: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"Erro CRÍTICO ao tentar enviar alerta para o Slack: {e}")
        raise  # Re-raise a exceção para que o Airflow saiba que o callback falhou


# --- FIM: FUNÇÕES DE CALLBACK DO SLACK ---