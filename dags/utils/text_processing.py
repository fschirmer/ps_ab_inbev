# /opt/airflow/dags/utils/text_processing.py

import unicodedata
import re
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


def to_ascii_safe_python(text: str) -> str:
    """
    Normaliza uma string para conter apenas caracteres ASCII seguros (a-z, 0-9, _),
    substituindo espaços e outros caracteres não permitidos por underscores.
    Esta é a versão Python pura, para uso geral ou dentro de UDFs PySpark.
    """
    if not isinstance(text, str):
        text = str(text)

    # 1. Normaliza para a forma de decomposição NFKD (separa o caractere base do diacrítico)
    #    e codifica para ASCII ignorando o que não é ASCII (removendo diacríticos).
    #    Em seguida, decodifica de volta para uma string Python (Unicode).
    normalized_and_ascii = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode(
        'ascii')

    # 2. Converte a string resultante para minúsculas
    lower_case_text = normalized_and_ascii.lower()

    # 3. Substitui qualquer caractere que NÃO seja a-z, 0-9 ou _ por um underscore.
    cleaned_text = re.sub(r'[^a-z0-9_]', '_', lower_case_text)

    # 4. Opcional: Remove underscores duplicados ou no início/fim, para maior limpeza
    cleaned_text = re.sub(r'__+', '_', cleaned_text)  # Substitui múltiplos underscores por um único
    cleaned_text = cleaned_text.strip('_')  # Remove underscores do início e fim

    return cleaned_text


# Define a função Python como um UDF para ser usada no PySpark
# Importante: o decorator @udf e os tipos devem ser importados do pyspark.sql
@udf(returnType=StringType())
def to_ascii_safe_spark_udf(text: str) -> str:
    """
    UDF do PySpark para normalizar strings para caracteres ASCII seguros,
    substituindo caracteres não permitidos por underscores.
    """
    return to_ascii_safe_python(text)
