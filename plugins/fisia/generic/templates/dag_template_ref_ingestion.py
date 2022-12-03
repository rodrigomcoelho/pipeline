import io
import json
from collections import ChainMap
from copy import deepcopy
from datetime import datetime, timedelta
from os.path import dirname, join, realpath

import libs.connection.query as qr
import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from libs.connection.notify import task_fail_slack_alert

# Informações da DAG
VERSION = "## VERSION ##"
BU = "## BU ##"
LAYER = "## LAYER ##"
DAG_NAME = "## DAG_NAME ##"
DOMAIN = "## DOMAIN ##"
REPOSITORY = "## REPOSITORY ##"
SCHEDULE_INTERVAL = "## SCHEDULE_INTERVAL ##"

DAG_CONTEXT = DAG_NAME.replace("_", "-")
TAGS = [
    tag.upper() for tag in [f"HB-v{str(VERSION)}", "AE", BU, LAYER, DOMAIN, REPOSITORY]
]

# Variáveis de configuração da pipeline
INFO_DAG = Variable.get("INFO_DEFAULT_AE", deserialize_json=True)
# DagPatterns.get_custom_variable('INFO_DEFAULT_AE')

with open(join(dirname(realpath(__file__)), "config", f"{DAG_NAME}.yaml"), "r") as file:
    cfg_ingestion = [
        tbl for tbl in yaml.safe_load(file.read()) if tbl["ACTIVE"] == "1"
    ][0]


# Variáveis de configuração da ingestão
PROJECT_ID = INFO_DAG["BU"][BU]
PROJECT = Variable.get("GCP_PROJECT_ID")
# DEFAULT_RANGE_INGESTION = int(INFO_DAG['DEFAULT_RANGE_INGESTION'])
FLG_SCHEDULER_ACTIVATED = bool(int(INFO_DAG["FLG_SCHEDULER_ACTIVATED"]))
TASK_TIMEOUT = float(INFO_DAG["TIMEOUT_TASK"])

default_args = {
    "start_date": datetime.fromisoformat("2022-07-01"),
    "retries": 2,
    "owner": "analytics-engineer",
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_fail_slack_alert,
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=(FLG_SCHEDULER_ACTIVATED or None) and SCHEDULE_INTERVAL,
    max_active_runs=1,
    start_date=datetime.fromisoformat("2022-07-01"),
    catchup=False,
    tags=TAGS,
    concurrency=8,
)


# Definindo as tarefas que a DAG vai executar, nesse caso a execucao de dois programas Python, chamando sua execucao por comandos bash
initial_vars = DummyOperator(task_id="start_job", retries=3)
ref_deps = dict(ChainMap(*cfg_ingestion["DEPENDENCIES"]))
deps = {
    key: INFO_DAG["BU"][value[0]] + "." + INFO_DAG["LAYER"][value[1]] + "." + key
    for key, value in ref_deps.items()
}
deps["REF_TABLE"] = (
    INFO_DAG["BU"][BU]
    + "."
    + INFO_DAG["LAYER"][LAYER]
    + "."
    + cfg_ingestion["REF_TABLE"]
)

complete = DummyOperator(task_id="All_jobs_completed")


def update_bq_ingestion(sql, project, table, step, **kwargs):

    return PythonOperator(
        task_id="update_table_{}_{}".format(table, step),
        python_callable=qr.running_bq,
        op_kwargs={"sql": sql, "project": project},
        dag=dag,
    )


schema_contract = {
    "id": "",
    "name": "",
    "schema_contract": {
        "type": "bigquery",
        "description": "Tabela Dimensao Cor",
        "partition": {
            "type": "",
            "config": {"field": "", "type": "", "require_partition_filter": False},
        },
        "properties": [],
    },
    "metadata": {
        "data_owner": "Data Engineering",
        "data_owner_email": "dataengineering@gruposbf.com.br",
        "data_custodian": "Data Engineering",
        "data_custodian_email": "dataengineering@gruposbf.com.br",
        "ingestion_method": "batch",
        "ingestion_cron": "",
        "upstream_lineage": [],
        "source_format": "native",
        "bu": "cnto",
        "bu_code": "0000",
        "tags": [],
    },
}


def burn_generate(**kwargs):
    project = kwargs["project_id"]
    _info_dag = kwargs["info_dag"]
    schema_contract = kwargs["schema_contract"]
    cfg_ingestion = kwargs["cfg_ingestion"]

    client = bigquery.Client(project=project)
    table_fullname = (
        _info_dag["BU"][BU]
        + "."
        + _info_dag["LAYER"][LAYER]
        + "."
        + cfg_ingestion["REF_TABLE"]
    )
    table = client.get_table(table_fullname)
    f = io.StringIO("")
    client.schema_to_json(table.schema, f)
    schema_bq = json.loads(f.getvalue())
    # print(schema_bq)

    json_structure = deepcopy(schema_contract)
    json_structure["name"] = cfg_ingestion["REF_TABLE"]
    json_structure["id"] = f"{BU.lower()}.analytics_engineering.prd.{table_fullname}.v1"

    json_structure["schema_contract"]["properties"] = []
    json_structure["metadata"]["tags"] = [DOMAIN.lower()]
    for i, itens in enumerate(schema_bq, start=1):
        json_structure["schema_contract"]["partition"] = {
            "type": "time",
            "config": {
                "field": cfg_ingestion["PARTITION"],
                "type": cfg_ingestion["TYPE_PARTITION"],
                "require_partition_filter": False,
            },
        }
        json_structure["schema_contract"]["properties"].append(
            {
                "field_name": itens.get("name", ""),
                "type": itens.get("type", ""),
                "mode": "NULLABLE",
                "description": itens.get("description", ""),
                "deidentify": {},
                "policyTags": {},
                "great_expectation": [],
            }
        )
    json_structure["metadata"]["upstream_lineage"].extend(
        [
            _info_dag["BU"][value[0]] + "." + _info_dag["LAYER"][value[1]] + "." + key
            for key, value in ref_deps.items()
            if key != "REF_TABLE"
        ]
    )
    return print(json_structure)


prev = initial_vars

for i, step in enumerate(cfg_ingestion["WORKFLOW"], start=1):
    if str(step["TYPE"]).casefold() == str("EXECUTE").casefold():
        sql = str(step["QUERY"]).format(**deps)
        step = step["QUERY"].split(" ")[0]
        up_step = update_bq_ingestion(sql, PROJECT_ID, cfg_ingestion["REF_TABLE"], step)
        prev >> up_step
        prev = up_step


if bool(int(INFO_DAG["FLG_SCHEDULER_ACTIVATED"])) is False:
    burnwood = PythonOperator(
        task_id="printContractBW",
        python_callable=burn_generate,
        op_kwargs={
            "project_id": PROJECT_ID,
            "info_dag": INFO_DAG,
            "schema_contract": schema_contract,
            "cfg_ingestion": cfg_ingestion,
        },
        dag=dag,
    )
else:
    burnwood = DummyOperator(task_id="printContractBW__DISABLED")

prev >> burnwood >> complete
