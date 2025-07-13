https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

1. O script acima foi estruturado em helpers/airflow-docker-compose.sh
2. criar o Docker file
3. criar o requirements

encontrado espaço em país (united states)
erro por causa de caracteres especiais como karten estado da austria com trema nmo segundo a
criar função para limpar mas também para notificar de caracteres não tratados
zipcodes with name
https://download.geonames.org/export/zip/

configurar google chat para receber notificação

slack
criar um app from scrath
https://api.slack.com/apps
incoming webhooks
criar conexão no airflow

redis foi adicionado no requirements mas precisa conferir

conexoa spark_defaul

docker exec -it ps_ab_inbev-airflow-worker-1 airflow connections add spark_default \
    --conn-type spark \
    --conn-host spark-master \
    --conn-port 7077 \
    --conn-extra '{"deploy_mode": "client", "spark_binary": "/opt/bitnami/spark/bin/spark-submit", "conf": {"spark.driver.host": "0.0.0.0"}}'

docker exec -it ps_ab_inbev-airflow-worker-1 airflow connections add slack_webhook \
    --conn-type slack_webhook \
    --conn-host "https://hooks.slack.com/services/" \
    --conn-schema "https" \
    --conn-password "xcxcxc" # O token do webhook é geralmente colocado no campo password ou extra

{
  "timeout": null,
  "proxy": null
}

https://hooks.slack.com/services/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

* MELHORIAS

Campos novos entrando como string (Este é importante[breweries_20250712_200101_561102.json](data_lake/bronze/breweries/data/year%3D2025/month%3D07/day%3D12/breweries_20250712_200101_561102.json))
limpeza de dados
Otimizar leitura de API
ZORDER, VACUUM 

download de arquivos para limpeza dos dados

eu alterei apenas dois dados de localidade porque não sei o quão confiável é o dado, 
então apenas demonstrei, ams entendo que a camada silver deve ter o dado tratado para consumo


1. Limpar dados (pend)
2. Função ler texto fora (ok)
3. Queries em arquivos (ok)
4. Documentacao
5. Desenho da arquitetura 
6. Remover jobs Spark da pasta Dags (ok)
7. Caracteres permitidos para partição
8. Colunas novas