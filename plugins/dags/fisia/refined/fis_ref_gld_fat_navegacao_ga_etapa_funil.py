import sys
from datetime import datetime, timedelta
from os.path import dirname, join, realpath

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

sys.path.append("/home/airflow/gcs/dags/data_engineering/")
import plugins.humbill.connection.query as qr

# Informações da DAG
PROJECT = Variable.get("GCP_PROJECT_FIS_AE")
LAYER = "REFINED"
DAG_NAME = "fis_ref_gld_fat_navegacao_ga_etapa_funil"
TAGS = ["AE", "FIS", "REFINED", "GA", "NAVEGACAO", "ETAPA", "FUNIL", "CLIENTE"]
SCHEDULE_INTERVAL = "40 12 * * *"
# Versão 1.1

# Variaveis para o SQL
CAMADA = Variable.get("GCP_LAYER_FIS_AE")

# Leitura do arquivo json que contém as tabelas que iremos integrar
with open(join(dirname(realpath(__file__)), "config", f"{DAG_NAME}.yaml")) as file:
    cfg_ingestion = [
        tbl for tbl in yaml.safe_load(file.read()) if tbl["ACTIVE"] == "1"
    ][0]
dag = DAG(
    default_args={
        "owner": "AE",
    },
    dag_id=DAG_NAME,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    start_date=datetime(2022, 11, 8, 0, 0),
    dagrun_timeout=timedelta(minutes=90),
    tags=TAGS,
)
sql = cfg_ingestion["SQL"].format(PROJECT=PROJECT, CAMADA=CAMADA)
# Definindo as tarefas que a DAG vai executar, nesse caso a execucao de dois programas Python, chamando sua execucao por comandos bash
dummy_operator = DummyOperator(task_id="start_job", retries=3)

ingestao = PythonOperator(
    task_id="task_" + DAG_NAME,
    python_callable=qr.running_bq,
    op_kwargs={"sql": sql, "project": PROJECT},
    dag=dag,
)

dummy_operator >> ingestao
