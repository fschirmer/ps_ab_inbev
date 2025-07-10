from airflow.decorators import dag, task
from datetime import datetime, timezone # Importe datetime e opcionalmente timezone

import os # Certifique-se de que 'os' está importado aqui
# Sua função de busca e salvamento da API
# (Coloque a função fetch_and_save_breweries_to_bronze aqui ou importe-a de um módulo Python separado dentro de 'plugins' ou 'dags')

# Defina o diretório de saída para a camada Bronze
BRONZE_LAYER_PATH = "/opt/airflow/data_lake/bronze"

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
                return

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
            except IOError as e:
                print(f"Erro ao salvar o arquivo JSON: {e}")
                raise # Re-raise para que o Airflow marque a tarefa como falha

        _fetch_and_save_breweries_to_bronze(BRONZE_LAYER_PATH)

    fetch_data_to_bronze_task = fetch_data_to_bronze()

# Instanciar o DAG
open_brewery_data_pipeline()