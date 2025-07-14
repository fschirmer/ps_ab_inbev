# Projeto de Pipeline de Dados de Cervejarias Abertas

Este projeto implementa um pipeline de dados ETL (Extract, Transform, Load) para coletar e processar informações de cervejarias da [Open Brewery DB API](https://www.openbrewerydb.org/). Ele utiliza uma arquitetura baseada em **Data Lakehouse** com camadas Bronze, Silver e Gold, orquestrada pelo **Apache Airflow** e processada pelo **Apache Spark** com **Delta Lake**. Dados de enriquecimento são integrados a partir de um arquivo CSV de códigos postais Geonames.

---

## Arquitetura do Projeto

A arquitetura do pipeline é composta pelos seguintes componentes:

* **Apache Airflow:** Orquestra e agenda as tarefas do pipeline ETL.
    * **PostgreSQL:** Banco de dados de metadados do Airflow.
    * **Redis:** Usado como broker para o Celery Executor do Airflow, facilitando a escalabilidade das tarefas.
* **Apache Spark:** Motor de processamento distribuído para as transformações de dados.
    * **Delta Lake:** Formato de armazenamento no Data Lakehouse, proporcionando transações ACID, versionamento de dados e esquemas evolutivos.
* **Camadas do Data Lakehouse:**
    * **Bronze:** Armazena os dados brutos e originais da API, sem transformações.
    * **Silver:** Dados limpos, padronizados e enriquecidos com informações de códigos postais Geonames.
    * **Gold:** Dados agregados e prontos para consumo por ferramentas de BI ou dashboards.

---

# Projeto de Pipeline de Dados de Cervejarias Abertas

Este projeto implementa um pipeline de dados ETL (Extract, Transform, Load) para coletar e processar informações de cervejarias da [Open Brewery DB API](https://www.openbrewerydb.org/). Ele utiliza uma arquitetura baseada em **Data Lakehouse** com camadas Bronze, Silver e Gold, orquestrada pelo **Apache Airflow** e processada pelo **Apache Spark** com **Delta Lake**. Dados de enriquecimento são integrados a partir de um arquivo CSV de códigos postais Geonames.

---

## Arquitetura do Projeto

A arquitetura do pipeline é composta pelos seguintes componentes:

* **Apache Airflow:** Orquestra e agenda as tarefas do pipeline ETL.
    * **PostgreSQL:** Banco de dados de metadados do Airflow.
    * **Redis:** Usado como broker para o Celery Executor do Airflow, facilitando a escalabilidade das tarefas.
* **Apache Spark:** Motor de processamento distribuído para as transformações de dados.
    * **Delta Lake:** Formato de armazenamento no Data Lakehouse, proporcionando transações ACID, versionamento de dados e esquemas evolutivos.
* **Camadas do Data Lakehouse:**
    * **Bronze:** Armazena os dados brutos e originais da API, sem transformações.
    * **Silver:** Dados limpos, padronizados e enriquecidos com informações de códigos postais Geonames.
    * **Gold:** Dados agregados e prontos para consumo por ferramentas de BI ou dashboards.

## Estrutura do Projeto

A estrutura de diretórios do projeto reflete as melhores práticas para pipelines de dados e desenvolvimento de aplicações Spark e Airflow:

```
.
├── dags/
│   ├── open_brewery_data_pipeline.py  # Definição principal da DAG do Airflow
│   └── utils/
│       ├── text_processing.py         # Utilitários para limpeza de texto
│       └── metadata_utils.py          # Utilitários para gerenciar metadados
├── data/
│   └── geonames-postal-code.csv       # Arquivo CSV para enriquecimento de dados (Geonames)
├── data_lake/
│   ├── bronze/                        # Camada Bronze do Data Lakehouse
│   ├── silver/                        # Camada Silver do Data Lakehouse
│   └── gold/                          # Camada Gold do Data Lakehouse
├── spark/
│   ├── breweries/
│   │   ├── jobs/
│   │   │   ├── fetch_data_bronze.py   # Script Python para extração da camada Bronze (não Spark)
│   │   │   ├── silver.py              # Script Spark para transformação da camada Silver
│   │   │   └── gold.py                # Script Spark para transformação da camada Gold
│   │   └── queries/
│   │       ├── queries_silver_breweries.py # Queries SQL/Spark para transformação Silver
│   │       └── queries_gold_breweries.py   # Queries SQL/Spark para transformação Gold
├── Dockerfile.Spark                   # Dockerfile para construir a imagem customizada do Spark
├── docker-compose.yml                 # Definição dos serviços Docker para o ambiente Airflow/Spark
├── README.md                          # Este arquivo
├── .env.example                       # Exemplo de arquivo de variáveis de ambiente
└── requirements.txt                   # Dependências Python do projeto
```

## Configuração do Ambiente

Este projeto utiliza Docker Compose para configurar um ambiente isolado com todos os serviços necessários.

### Pré-requisitos

* **Docker Desktop** (ou Docker Engine e Docker Compose) instalado.
* **Git** para clonar o repositório.

### Passos de Configuração

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/fschirmer/ps_ab_inbev.git
    cd ps_ab_inbev
    ```
2.  **Crie o arquivo `.env`:**
    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    # Gere sua chave Fernet com Python: from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())
    echo "FERNET_KEY=SUA_CHAVE_FERNET_GERADA_AQUI" >> .env
    ```
    Você pode ajustar `AIRFLOW_UID` (para evitar problemas de permissão de arquivo), `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`, etc.
3. **Inicie o ambiente Docker Compose:**
    ```bash
    docker compose build --no-cache 
    docker compose up -d
    ```
    Na primeira execução, isso pode levar alguns minutos, pois o Docker fará o download das imagens e construirá as customizadas.
4. **Acesse a UI do Airflow:**
    Após os contêineres estarem em execução (pode levar alguns minutos para o Airflow inicializar), acesse a interface do usuário do Airflow em `http://localhost:8080`.
    Use as credenciais que foram **configuradas no `docker-compose.yml`** (padrão: `airflow`/`airflow`) para fazer login. Elas são definidas durante a inicialização do Airflow.
5. **Criação da Conexão do Spark:**
    Para que as DAGs do Airflow possam se comunicar com o Spark, é necessário criar a seguintes conexão. Execute este comando no seu terminal, após o ambiente Docker Compose estar completamente inicializado e os contêineres do Airflow estarem em execução.

    **Nota:** Substitua `ps_ab_inbev-airflow-worker-1` pelo nome real do seu contêiner `airflow-worker` (você pode verificar com `docker ps`).

    1.  **Conexão Spark (`spark_default`)**:
        Esta conexão permite que o Airflow submeta jobs Spark para o Spark Master.

        ```bash
        docker exec -it ps_ab_inbev-airflow-worker-1 airflow connections add spark_default \
            --conn-type spark \
            --conn-host spark://spark-master \
            --conn-port 7077 \
            --conn-extra '{"deploy_mode": "client", "spark_binary": "/opt/bitnami/spark/bin/spark-submit", "conf": {"spark.driver.host": "0.0.0.0"}}'
        ```
6. **Notificações:**
    A dag está configurada para enviar notificações via Slack caso ocorra um erro.

   1. **Conexão Slack Webhook (`slack_webhook`)**:
         Siga os passos do link abaixo para criar um canal e habilitar um app.
         https://api.slack.com/messaging/webhooks
   2. **Conexão Slack Webhook (`slack_webhook`)**:
        Esta conexão é usada para enviar notificações do pipeline para um canal do Slack.

        ```bash
        docker exec -it ps_ab_inbev-airflow-worker-1 airflow connections add slack_webhook \
            --conn-type slackwebhook \
            --conn-host "https://hooks.slack.com/services/" \
            --conn-schema "https" \
            --conn-password "xcxcxc" # O token do webhook é geralmente colocado no campo password ou extra. SUBSTITUA "xcxcxc" PELO SEU TOKEN REAL DO SLACK!
        ```

---

## Executando o Pipeline

O pipeline `open_brewery_data_pipeline` deve aparecer na UI do Airflow. Ele está configurado para não ter um agendamento fixo (`schedule=None`), então você precisará dispará-lo manualmente.

1.  **Despausar a DAG:** Na UI do Airflow, localize `open_brewery_data_pipeline` e ative o botão de "Toggle" para despausá-la.
2.  **Disparar a DAG:** Clique no botão "Trigger DAG" (ícone de play) para iniciar uma nova execução.

---

## Detalhes do Pipeline

### Camada Bronze (Extração)

* **Script:** `spark/breweries/jobs/fetch_data_bronze.py`
* **Função:** Coleta dados de cervejarias da Open Brewery DB API usando requisições HTTP.
* **Armazenamento:** Salva os dados brutos como arquivos JSON particionados por data (`year=/month=/day=`) no diretório `data_lake/bronze/breweries/data/`.
* **Metadados:** Registra o timestamp do arquivo no diretório `data_lake/bronze/breweries/metadata/`.

### Camada Silver (Transformação e Enriquecimento)

* **Script:** `spark/breweries/jobs/silver.py`
* **Queries:** `spark/breweries/queries/queries_silver_breweries.py`
* **Enriquecimento:** Lê o arquivo `data/geonames-postal-code.csv` e faz um join com os dados da Bronze, utilizando o `postal_code` para adicionar informações geográficas e administrativas.
* **Limpeza:** Aplica limpeza e padronização a diversas colunas dos dados das cervejarias (ex: normalização de strings, tratamento de nulos).
* **Armazenamento:** Salva os dados limpos e enriquecidos como tabelas Delta particionadas por `country` em `data_lake/silver/breweries/`.

### Camada Gold (Agregação)

* **Script:** `spark/breweries/jobs/gold.py`
* **Queries:** `spark/breweries/queries/queries_gold_breweries.py`
* **Transformação:** Agrega e sumariza os dados da camada Silver para criar visões de alto nível, ideais para análise e BI.
* **Armazenamento:** Salva os dados agregados como tabelas Delta em `data_lake/gold/breweries/`.

### Links de Apoio

* **Airflow:** https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html


Sinta-se à vontade para explorar, modificar e estender este projeto!