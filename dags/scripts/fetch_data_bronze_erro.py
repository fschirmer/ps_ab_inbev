import os
import requests
import json
from datetime import datetime, timezone
from utils.metadata_utils import save_timestamp_to_metadata_file, read_timestamp_from_metadata_file


def fetch_and_save_breweries_to_bronze(output_dir: str):
    """
    Busca dados de cervejarias da Open Brewery DB API e os salva na camada Bronze.

    Args:
        output_dir (str): O diretório base onde os dados brutos serão salvos.

    Returns:
        str: O caminho completo do arquivo JSON salvo.

    Raises:
        requests.exceptions.RequestException: Se ocorrer um erro na requisição HTTP.
        IOError: Se ocorrer um erro ao salvar o arquivo.
        ValueError: Se nenhum dado for recuperado da API.
    """
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

            # Forçar erro para testar Slack
            resultado = 1 / 0
            print(resultado)

            if not breweries:
                print(f"Nenhum dado retornado na página {page}. Fim da paginação.")
                break

            all_breweries_data.extend(breweries)
            print(f"Página {page} ({len(breweries)} cervejarias) buscada com sucesso.")
            page += 1

        except requests.exceptions.RequestException as req_err:
            print(f"Ocorreu um erro na requisição: {req_err}")
            raise

    if not all_breweries_data:
        print("Nenhum dado de cervejaria foi recuperado da API.")
        raise ValueError(
            "Nenhum dado de cervejaria foi recuperado da API para a camada Bronze."
        )

    now_utc = datetime.now(timezone.utc)

    timestamp_str = now_utc.strftime("%Y%m%d_%H%M%S_%f")

    file_name = f"breweries_{timestamp_str}.json"

    # Define as chaves de partição hierárquicas (ano, mês, dia)
    # Ex: year=2025/month=07/day=11
    year_partition = f"year={now_utc.year}"
    month_partition = f"month={now_utc.month:02d}"  # :02d garante dois dígitos (ex: 07)
    day_partition = f"day={now_utc.day:02d}"  # :02d garante dois dígitos (ex: 01)

    # Constrói o caminho completo da pasta particionada
    # Ex: /opt/airflow/data_lake/bronze/breweries/data/year=2025/month=07/day=11
    full_output_dir_with_partition = os.path.join(
        output_dir, "breweries", "data", year_partition, month_partition, day_partition
    )

    # Constrói o caminho completo do arquivo
    # Ex: /opt/airflow/data_lake/bronze/breweries/data/year=2025/month=07/day=11/breweries_20250711_221343_092307.json
    full_output_path = os.path.join(full_output_dir_with_partition, file_name)

    os.makedirs(full_output_dir_with_partition, exist_ok=True)

    try:
        with open(full_output_path, 'w', encoding='utf-8') as f:
            for record in all_breweries_data:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        print(f"Dados brutos salvos com sucesso na camada Bronze: {full_output_path}")
        print(f"Total de {len(all_breweries_data)} registros salvos.")

        BRONZE_METADATA_FILE_PATH = os.path.join(
            output_dir, "breweries", "metadata", "last_processed_timestamp.txt"
        )

        save_timestamp_to_metadata_file(timestamp_str, BRONZE_METADATA_FILE_PATH)

        return full_output_path
    except IOError as e:
        print(f"Erro ao salvar o arquivo JSON: {e}")
        raise


# if __name__ == "__main__":
#     # Defina o caminho base para seus metadados
#     METADATA_BASE_PATH = "/opt/airflow/data_lake/metadata"
#
#     # Exemplo para a camada Bronze
#     bronze_status_file = os.path.join(METADATA_BASE_PATH, "bronze", "open_brewery_api_status.txt")
#     current_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
#
#     print("\n--- Testando salvar timestamp para Bronze ---")
#     save_timestamp_to_metadata_file(current_timestamp, bronze_status_file)
#
#     # Exemplo para a camada Silver
#     silver_status_file = os.path.join(METADATA_BASE_PATH, "silver", "breweries_silver_status.txt")
#     # Em um cenário real, este timestamp viria do último dado processado da Bronze
#     last_processed_bronze_date = "20250710_235959"  # Exemplo de timestamp de uma partição Bronze
#     save_timestamp_to_metadata_file(last_processed_bronze_date, silver_status_file)

