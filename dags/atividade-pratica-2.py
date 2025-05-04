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

TEMP_DIR = "/tmp/etl_temp"

# step 1
def extract_data(**context):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'meu-bucket-dados'
    prefix = 'dados/raw/'

    os.makedirs(TEMP_DIR, exist_ok=True)

    # Simulando o uso de intervalo de datas
    data_inicio = context['params']['data_inicio']
    data_fim = context['params']['data_fim']

    # Baixando arquivos CSV e JSON
    arquivos = s3_hook.list_keys(bucket_name, prefix=prefix)
    for arquivo in arquivos:
        if any(arquivo.endswith(ext) for ext in ['.csv', '.json']):
            if data_inicio in arquivo or data_fim in arquivo:
                s3_hook.download_file(bucket_name, arquivo, local_path=TEMP_DIR)

# step 2
def transform_data():
    csv_files = glob.glob(f"{TEMP_DIR}/*.csv")
    json_files = glob.glob(f"{TEMP_DIR}/*.json")

    df_list = [pd.read_csv(f) for f in csv_files]
    df_csv = pd.concat(df_list, ignore_index=True)

    df_json = pd.DataFrame()
    for jf in json_files:
        with open(jf) as f:
            data = json.load(f)
            df = pd.json_normalize(data)
            df_json = pd.concat([df_json, df], ignore_index=True)

    df_merged = pd.merge(df_csv, df_json, how='inner', on='id')
    df_merged.to_csv(f"{TEMP_DIR}/dados_transformados.csv", index=False)

# step 3
def load_data():
    df = pd.read_csv(f"{TEMP_DIR}/dados_transformados.csv")
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    with open("scripts/insercao_template.sql") as f:
        insert_sql = f.read()

    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))
    conn.commit()

# end of pipeline
def cleanup():
    if os.path.exists(TEMP_DIR):
        for f in glob.glob(f"{TEMP_DIR}/*"):
            os.remove(f)
        os.rmdir(TEMP_DIR)

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2024, 1, 1)
}

with DAG(
    dag_id='etl_pipeline_completo',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Pipeline ETL com S3, Pandas e Postgres',
    params={"data_inicio": "2024-01-01", "data_fim": "2024-12-31"}
) as dag:

    task_extract = PythonOperator(
        task_id='extrair_dados',
        python_callable=extract_data,
        provide_context=True
    )

    task_transform = PythonOperator(
        task_id='transformar_dados',
        python_callable=transform_data
    )

    task_load = PythonOperator(
        task_id='carregar_dados',
        python_callable=load_data
    )

    task_cleanup = PythonOperator(
        task_id='limpeza_final',
        python_callable=cleanup
    )

    task_extract >> task_transform >> task_load >> task_cleanup
