#!/bin/bash

# Caminho base do projeto
export PROJECT_DIR=$(pwd)

# Diretórios do Airflow
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$PROJECT_DIR/plugins"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

echo "✅ Ambiente configurado com sucesso!"