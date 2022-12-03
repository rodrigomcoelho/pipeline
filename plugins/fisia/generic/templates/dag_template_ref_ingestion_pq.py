from datetime import datetime, timedelta
from os.path import dirname, join, realpath

from airflow import DAG
from libs.connection.notify import task_fail_slack_alert
from libs.generic.patterns.dag_ingestion_refined import (
    DagIngestionRefined as DagPatterns,
)

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
    tag.upper() for tag in [f"HB-v{str(VERSION)}", "DE", BU, LAYER, DOMAIN, REPOSITORY]
]

# Variáveis de configuração da pipeline
INFO_DAG = DagPatterns.get_custom_variable("INFO_DEFAULT_DAG", BU)
INFO_DATAPROC = DagPatterns.get_custom_variable("INFO_DEFAULT_DATAPROC", BU)
INFO_REF_INGESTION = DagPatterns.get_custom_variable("INFO_DEFAULT_REF_INGESTION", BU)
INFO_DATA_QUALITY = DagPatterns.get_custom_variable("INFO_DEFAULT_DATA_QUALITY", BU)

# Variáveis de configuração da ingestão
PROJECT_ID = INFO_DAG["PROJECT_ID"]
DEFAULT_RANGE_INGESTION = int(INFO_DAG["DEFAULT_RANGE_INGESTION"])
FLG_SCHEDULER_ACTIVATED = bool(int(INFO_DAG["FLG_SCHEDULER_ACTIVATED"]))
TASK_TIMEOUT = float(INFO_DAG[LAYER]["TIMEOUT_TASK"])

default_args = {
    "start_date": datetime.fromisoformat("2022-07-01"),
    "retries": 2,
    "owner": "data-engineer",
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_fail_slack_alert,
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=(FLG_SCHEDULER_ACTIVATED or None) and SCHEDULE_INTERVAL,
    max_active_runs=1,
    catchup=False,
    tags=TAGS,
    concurrency=8,
)

dag_patterns = DagPatterns(
    script_version=VERSION,
    layer=LAYER,
    domain=DOMAIN,
    dag_name=DAG_NAME,
    dag_context=DAG_CONTEXT,
    info_dataproc=INFO_DATAPROC,
    path_ingestion_config=join(
        dirname(realpath(__file__)), "config", f"{DAG_NAME}.yaml"
    ),
    info_ref_ingestion=INFO_REF_INGESTION,
    info_quality_ingestion=INFO_DATA_QUALITY,
    log_repository=REPOSITORY,
    task_timeout=TASK_TIMEOUT,
)

environment_setup = dag_patterns.consolidate_origins(pattern_variables=INFO_DAG["ID"])

dag_patterns.define_spark_jobs(
    ref_spark_job=INFO_REF_INGESTION["SPARK_JOB_PATH"],
    data_quality_spark_job=INFO_DATA_QUALITY["SPARK_JOB_PATH"],
)

initial_vars = dag_patterns.created_start_job(dag=dag)

dag_patterns.define_cluster(dag=dag)

complete_process = dag_patterns.complete_process(dag=dag)

for table in dag_patterns.cfg_ingestion:

    ingestion_ref = dag_patterns.ref_update_table(
        dag=dag,
        destination_table=table.REF_TABLE,
        cfg_table=table,
        destination_table_project_id=PROJECT_ID,
        destination_table_bucket=INFO_DAG[LAYER]["BUCKET_ID"],
        destination_table_dataset=INFO_DAG[LAYER]["DATASET"],
        create_table_if_needed=bool(int(INFO_DAG["FLG_CREATE_TABLE_IF_NEEDED"])),
        environment_setup=environment_setup,
        path_ingestion_control_partitions=str(
            INFO_DAG["INGESTION_CONTROL"]["PARTITIONS_FILENAME"]
        ).format(
            LAYER=INFO_DAG[LAYER]["DATASET"].lower(),
            DAG_NAME=DAG_NAME.lower(),
            TABLE_NAME=table.REF_TABLE,
        ),
        path_ingestion_control_count=str(
            INFO_DAG["INGESTION_CONTROL"]["COUNT_FILENAME"]
        ).format(
            LAYER=INFO_DAG[LAYER]["DATASET"].lower(),
            DAG_NAME=DAG_NAME.lower(),
            TABLE_NAME=table.REF_TABLE,
        ),
    )

    data_quality = dag_patterns.data_quality(
        dag=dag,
        destination_table=table.REF_TABLE,
        cfg_table=table,
        data_quality_info=INFO_DATA_QUALITY,
        destination_table_project_id=PROJECT_ID,
        destination_table_bucket=INFO_DAG[LAYER]["BUCKET_ID"],
        destination_table_dataset=INFO_DAG[LAYER]["DATASET"],
        path_ingestion_control_partitions=str(
            INFO_DAG["INGESTION_CONTROL"]["PARTITIONS_FILENAME"]
        ).format(
            LAYER=INFO_DAG[LAYER]["DATASET"].lower(),
            DAG_NAME=DAG_NAME.lower(),
            TABLE_NAME=table.REF_TABLE,
        ),
        path_ingestion_control_count=str(
            INFO_DAG["INGESTION_CONTROL"]["COUNT_FILENAME"]
        ).format(
            LAYER=INFO_DAG[LAYER]["DATASET"].lower(),
            DAG_NAME=DAG_NAME.lower(),
            TABLE_NAME=table.REF_TABLE,
        ),
    )

    load_data_to_bigquery = dag_patterns.load_data_to_bigquery(
        dag=dag,
        project_id=PROJECT_ID,
        bucket=INFO_DAG[LAYER]["BUCKET_ID"],
        dataset=INFO_DAG[LAYER]["DATASET"],
        table_name=table.REF_TABLE,
        partition=table.PARTITION,
        source_format=table.SOURCE_FORMAT,
        type_partition=table.TYPE_PARTITION,
        clustering_fields=table.CLUSTERED,
        create_if_needed=bool(int(INFO_DAG["FLG_CREATE_TABLE_IF_NEEDED"])),
    )

    ref_generate_script = dag_patterns.ref_generate_script(
        dag=dag,
        destination_table=table.REF_TABLE,
        cfg_table=table,
        destination_table_project_id=PROJECT_ID,
        destination_table_bucket=INFO_DAG[LAYER]["BUCKET_ID"],
        destination_table_dataset=INFO_DAG[LAYER]["DATASET"],
        create_table_if_needed=bool(int(INFO_DAG["FLG_CREATE_TABLE_IF_NEEDED"])),
        environment_setup=environment_setup,
    )

    (
        initial_vars
        >> dag_patterns.cluster.CLUSTER_CREATED
        >> ingestion_ref
        >> data_quality
        >> load_data_to_bigquery
        >> ref_generate_script
        >> dag_patterns.cluster.CLUSTER_DELETE
        >> complete_process
    )
