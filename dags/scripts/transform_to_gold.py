# /opt/airflow/dags/scripts/transform_to_gold.py

import sys # Importar sys para acesso aos argumentos da linha de comando
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from datetime import datetime

# A função principal que será chamada pelo script
def main():
    spark = SparkSession.builder.appName("BreweryGoldAggregation").getOrCreate()

    # Analisar os argumentos passados via spark-submit
    import argparse
    parser = argparse.ArgumentParser(description="Spark job for Silver to Gold aggregation.")
    parser.add_argument('--silver_layer_path', type=str, help='Path to the Silver layer input directory.')
    parser.add_argument('--gold_layer_path', type=str, help='Path to the Gold layer output directory.')
    args = parser.parse_args()

    silver_layer_path = args.silver_layer_path
    gold_layer_path = args.gold_layer_path

    try:
        # 1. Leitura dos dados da camada Silver
        df_silver = spark.read.parquet(silver_layer_path)
        print(f"Dados lidos com sucesso da camada Silver: {silver_layer_path}")
        df_silver.printSchema() # Imprima o schema para depuração e confirmação das colunas
        df_silver.show(5)

    except Exception as e:
        print(f"Erro ao ler o arquivo Silver: {e}")
        spark.stop() # Parar a sessão Spark em caso de erro
        raise # Re-raise para que o Airflow marque a tarefa como falha

    # 2. Agregação dos dados: quantidade de cervejarias por tipo e localização
    df_gold = df_silver.groupBy(
        # Renomeia as colunas para o formato desejado na Gold para particionamento e legibilidade
        col("country_partition").alias("country"),
        col("brewery_type")
    ).agg(
        # CORRIGIDO: Usar 'brewery_id' conforme definido na camada Silver
        count(col("brewery_id")).alias("quantity_of_breweries")
    ).withColumn(
        "created_at", lit(datetime.now())
    ).withColumn(
        "updated_at", lit(datetime.now())
    )

    print("Schema da camada Gold agregada:")
    df_gold.printSchema()
    df_gold.show(5)

    # 3. Escrita dos dados agregados na camada Gold
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(gold_layer_path, f"breweries_summary_{current_timestamp}.parquet")

    # Particiona usando os aliases 'country' e 'state_region' que foram criados no groupBy/select
    df_gold.write.mode("overwrite").parquet(output_path)

    print(f"Dados agregados salvos com sucesso na camada Gold: {output_path}")

    spark.stop()

# Este bloco garante que a função main() seja chamada quando o script for executado via spark-submit
if __name__ == "__main__":
    main()