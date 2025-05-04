from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
import json
import glob

# Diretório temporário usado para armazenar os arquivos baixados e processados
TEMP_DIR = "/tmp/etl_temp"

# ----------------------
# Etapa 1: Extração
# ----------------------
def extract_data(**context):
    """
    Esta função é responsável por extrair os dados brutos do S3.
    Utiliza parâmetros de data (início e fim) para selecionar os arquivos a serem baixados.
    """

    # Inicializa o hook de conexão com a AWS S3, usando uma conexão definida no Airflow
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Define o bucket e o prefixo onde os dados estão armazenados
    bucket_name = 'meu-bucket-dados'
    prefix = 'dados/raw/'

    # Garante que o diretório temporário existe
    os.makedirs(TEMP_DIR, exist_ok=True)

    # Recupera os parâmetros definidos no DAG para filtrar os arquivos por data
    data_inicio = context['params']['data_inicio']
    data_fim = context['params']['data_fim']

    # Lista todos os arquivos no bucket com o prefixo especificado
    arquivos = s3_hook.list_keys(bucket_name, prefix=prefix)

    # Faz o download apenas dos arquivos que forem CSV ou JSON e que contenham a data de interesse
    for arquivo in arquivos:
        if any(arquivo.endswith(ext) for ext in ['.csv', '.json']):
            if data_inicio in arquivo or data_fim in arquivo:
                s3_hook.download_file(bucket_name, arquivo, local_path=TEMP_DIR)

# ----------------------
# Etapa 2: Transformação
# ----------------------
def transform_data():
    """
    Esta função transforma os dados:
    - Lê todos os arquivos CSV e JSON baixados.
    - Junta os CSVs em um único DataFrame.
    - Concatena os JSONs em outro DataFrame.
    - Realiza um merge entre os dois, com base no campo 'id'.
    - Salva o resultado final em um novo arquivo CSV.
    """

    # Captura todos os arquivos CSV e JSON baixados no diretório temporário
    csv_files = glob.glob(f"{TEMP_DIR}/*.csv")
    json_files = glob.glob(f"{TEMP_DIR}/*.json")

    # Lê os arquivos CSV e une em um único DataFrame
    df_list = [pd.read_csv(f) for f in csv_files]
    df_csv = pd.concat(df_list, ignore_index=True)

    # Lê e concatena os arquivos JSON em outro DataFrame
    df_json = pd.DataFrame()
    for jf in json_files:
        with open(jf) as f:
            data = json.load(f)
            df = pd.json_normalize(data)
            df_json = pd.concat([df_json, df], ignore_index=True)

    # Realiza o merge entre os dois DataFrames com base no campo 'id'
    df_merged = pd.merge(df_csv, df_json, how='inner', on='id')

    # Salva o DataFrame final transformado em um novo CSV
    df_merged.to_csv(f"{TEMP_DIR}/dados_transformados.csv", index=False)

# ----------------------
# Etapa 3: Carga
# ----------------------
def load_data():
    """
    Esta função carrega os dados transformados para um banco de dados PostgreSQL.
    Utiliza um template SQL externo e executa os inserts linha por linha.
    """

    # Lê o arquivo transformado salvo na etapa anterior
    df = pd.read_csv(f"{TEMP_DIR}/dados_transformados.csv")

    # Inicializa o hook de conexão com o PostgreSQL
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Carrega o template SQL de inserção
    with open("scripts/insercao_template.sql") as f:
        insert_sql = f.read()

    # Executa a inserção dos dados linha por linha
    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))
    conn.commit()

# ----------------------
# Etapa Final: Limpeza
# ----------------------
def cleanup():
    """
    Esta função remove todos os arquivos e diretórios temporários criados durante o processo.
    Serve para liberar espaço em disco e evitar resíduos de execuções anteriores.
    """
    if os.path.exists(TEMP_DIR):
        for f in glob.glob(f"{TEMP_DIR}/*"):
            os.remove(f)
        os.rmdir(TEMP_DIR)

# ----------------------
# Definições do DAG
# ----------------------
default_args = {
    'owner': 'airflow',                         # Proprietário do DAG
    'retries': 2,                               # Número de tentativas em caso de falha
    'retry_delay': timedelta(minutes=3),        # Intervalo entre as tentativas
    'start_date': datetime(2024, 1, 1)          # Data de início (não retroage se catchup=False)
}

# Criação da DAG principal com todas as tasks conectadas em ordem
with DAG(
    dag_id='etl_pipeline_completo',
    default_args=default_args,
    schedule_interval=None,                     # DAG só roda manualmente (ou com trigger externo)
    catchup=False,                              # Evita execução retroativa
    description='Pipeline ETL com S3, Pandas e Postgres',
    params={"data_inicio": "2024-01-01", "data_fim": "2024-12-31"}  # Parâmetros usados na extração
) as dag:

    # Task 1: Extração dos dados do S3
    task_extract = PythonOperator(
        task_id='extrair_dados',
        python_callable=extract_data,
        provide_context=True
    )

    # Task 2: Transformação dos dados com Pandas
    task_transform = PythonOperator(
        task_id='transformar_dados',
        python_callable=transform_data
    )

    # Task 3: Carga dos dados no banco de dados PostgreSQL
    task_load = PythonOperator(
        task_id='carregar_dados',
        python_callable=load_data
    )

    # Task 4: Limpeza dos arquivos temporários
    task_cleanup = PythonOperator(
        task_id='limpeza_final',
        python_callable=cleanup
    )

    # Encadeamento das tarefas (ordem de execução)
    task_extract >> task_transform >> task_load >> task_cleanup
