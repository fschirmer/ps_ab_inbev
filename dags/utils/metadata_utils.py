import os
import sys

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

