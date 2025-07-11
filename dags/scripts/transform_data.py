# /opt/airflow/dags/scripts/transform_data.py

# Importe sua UDF de text_processing
# Note que a importação aqui pode ser mais simples porque o script será submetido
# com py_files, o que adiciona o diretório/zip ao PYTHONPATH.
from text_processing import to_ascii_safe_spark_udf

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim
from pyspark.sql.types import StringType
import sys

def run_transform(bronze_file_path: str):
    """
    Função principal para executar a transformação Spark.
    """
    spark = SparkSession.builder.appName("BreweryDataTransform").getOrCreate()

    df_bronze = spark.read.json(bronze_file_path)

    df_silver = df_bronze.select(
        col("id").cast(StringType()).alias("brewery_id"),
        col("name").alias("brewery_name"),
        col("brewery_type"),
        col("street"),
        col("address_2"),
        col("address_3"),
        col("city"),
        col("state"),
        col("postal_code"),
        col("country"),
        col("longitude"),
        col("latitude"),
        col("phone"),
        col("website_url"),
        col("state_province")
    )

    # Aplicar a função de limpeza diretamente nas colunas para particionamento usando o UDF
    df_silver = df_silver.withColumn(
        "country_partition",
        to_ascii_safe_spark_udf(col("country"))
    # ).withColumn(
    #     "state_partition",
    #     to_ascii_safe_spark_udf(col("state_province"))
    )

    # Tratar casos onde state_region/country pode ser nulo ou vazio para a partição
    df_silver = df_silver.withColumn(
        "state_province",
        when(col("state_province").isNull() | (trim(col("state_province")) == ""),
             lit("unknown"))
        .otherwise(col("state_province"))
    ).withColumn(
        "country_partition",
        when(col("country_partition").isNull() | (trim(col("country_partition")) == ""),
             lit("unknown"))
        .otherwise(col("country_partition"))
    )

    # Definir o caminho de saída (assumindo que o bronze_file_path ajuda a derivar a data)
    # Exemplo: /opt/airflow/data_lake/silver/breweries/country_partition=USA/state_partition=new_york/part-....parquet
    output_path = "/opt/airflow/data_lake/silver/" + \
                  f"processed_date={datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # .partitionBy("country_partition", "state_province") \
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("country_partition") \
        .parquet(output_path)

    print(f"Dados Silver salvos em: {output_path}")

    spark.stop()

# Este bloco verifica se o script está sendo executado diretamente pelo spark-submit
if __name__ == "__main__" :
    # Arguments passed to the script via spark-submit's application_args
    # sys.argv[0] é o nome do script, sys.argv[1] será o bronze_file_path
    if len(sys.argv) > 1:
        bronze_file_path_arg = sys.argv[1]
        run_transform(bronze_file_path_arg)
    else:
        print("Uso: spark-submit transform_data.py <caminho_do_arquivo_bronze>")
        sys.exit(1)