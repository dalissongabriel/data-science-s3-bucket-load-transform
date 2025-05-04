
# 📘 DOCUMENTAÇÃO DO PROJETO

### **Objetivo**

Automatizar o fluxo de tratamento de dados usando Apache Airflow, integrando AWS S3, Pandas e PostgreSQL.

### **Etapas do Pipeline**

1. **Extração (extract_data)**:
- Utiliza S3Hook para baixar arquivos .csv e .json de um bucket no S3.
- Usa parâmetros de data (data_inicio e data_fim) para filtrar os arquivos desejados.
- Todos os arquivos são salvos localmente em um diretório temporário.

2. **Transformação (transform_data):**
- Une todos os arquivos CSV em um único DataFrame com o pandas.concat.
- Lê os JSONs com json.load e os transforma em DataFrame com json_normalize.
- Realiza um merge entre os dois DataFrames com base na coluna id.
- O DataFrame final é salvo como um CSV (dados_transformados.csv).

3. **Carga (load_data):**
- Lê o CSV final com pandas.
- Usa PostgresHook para conectar-se ao banco PostgreSQL.
- Executa um script SQL de inserção (modelo insercao_template.sql) para cada linha do DataFrame.

4. **Limpeza (cleanup):**
- Remove todos os arquivos temporários e o diretório /tmp/etl_temp após a conclusão da DAG.

**Configurações Técnicas**

- **Retries**: Cada task tenta até 2 vezes em caso de falha, com 3 minutos de intervalo.
- **catchup=False**: Garante que execuções anteriores à data atual não sejam processadas automaticamente.
- **schedule_interval=None**: O DAG só será executado manualmente.
- **Diretórios Temporários**: Isolamento dos arquivos para controle do processo e limpeza ao final.
- **Módulos instalados**: As dependencias do python instaladas foram:
	- apache-airflow==2.8.1
	- pandas
	- boto3
	- psycopg2-binary


**Explicação da Estrutura**

- A DAG foi dividida em tarefas com responsabilidades únicas, seguindo o padrão ETL (Extract, Transform, Load). 
- Parâmetros foram incluídos no params do DAG para flexibilizar o intervalo de dados processados.
- A lógica está modularizada para facilitar a manutenção e possíveis melhorias (como paralelização ou validações).

