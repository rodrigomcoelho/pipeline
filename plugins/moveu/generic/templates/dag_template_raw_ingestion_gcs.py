from datetime import datetime, timedelta
from os.path import dirname, join, realpath

from airflow import DAG
from libs.connection.notify import task_fail_slack_alert
from libs.generic.patterns.dag_ingestion_gcs_patterns import (
    DagIngestionGcsPatterns as DagPatterns,
)

# Informações da DAG
VERSION = "## VERSION ##"
BU = "## BU ##"
RAW_LAYER = "## LAYER ##"
TRD_LAYER = "TRUSTED"
DAG_NAME = "## DAG_NAME ##"
DOMAIN = "## DOMAIN ##"
REPOSITORY = "## REPOSITORY ##"
SCHEDULE_INTERVAL = "## SCHEDULE_INTERVAL ##"

DAG_CONTEXT = DAG_NAME.replace("_", "-")
TAGS = [
    tag.upper()
    for tag in [
        f"HB-v{str(VERSION)}",
        "DE",
        BU,
        RAW_LAYER,
        TRD_LAYER,
        DOMAIN,
        REPOSITORY,
        "INGESTION_FILE",
    ]
]

# Variáveis de configuração da pipeline
INFO_DAG = DagPatterns.get_custom_variable("INFO_DEFAULT_DAG", BU)
INFO_DATAPROC = DagPatterns.get_custom_variable("INFO_DEFAULT_DATAPROC", BU)
INFO_RAW_INGESTION = DagPatterns.get_custom_variable(
    "INFO_DEFAULT_RAW_INGESTION_FILES", BU
)
INFO_TRD_INGESTION = DagPatterns.get_custom_variable("INFO_DEFAULT_TRD_INGESTION", BU)
INFO_DATA_QUALITY = DagPatterns.get_custom_variable("INFO_DEFAULT_DATA_QUALITY", BU)

# Variáveis de configuração da ingestão
PROJECT_ID = INFO_DAG["PROJECT_ID"]
FLG_SCHEDULER_ACTIVATED = bool(int(INFO_DAG["FLG_SCHEDULER_ACTIVATED"]))
TASK_TIMEOUT = float(INFO_DAG[RAW_LAYER]["TIMEOUT_TASK"])
DEFAULT_RANGE_INGESTION = int(INFO_DAG["DEFAULT_RANGE_INGESTION"])

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
    layer=RAW_LAYER,
    domain=DOMAIN,
    dag_name=DAG_NAME,
    dag_context=DAG_CONTEXT,
    info_dataproc=INFO_DATAPROC,
    info_raw_ingestion=INFO_RAW_INGESTION,
    info_trd_ingestion=INFO_TRD_INGESTION,
    info_quality_ingestion=INFO_DATA_QUALITY,
    path_ingestion_config=join(
        dirname(realpath(__file__)), "config", f"{DAG_NAME}.yaml"
    ),
    task_timeout=TASK_TIMEOUT,
    log_repository=REPOSITORY,
)

dag_patterns.define_spark_jobs(
    raw_spark_job=INFO_RAW_INGESTION["SPARK_JOB_PATH"],
    trd_spark_job=INFO_TRD_INGESTION["SPARK_JOB_PATH"],
    data_quality_spark_job=INFO_DATA_QUALITY["SPARK_JOB_PATH"],
)

initial_vars = dag_patterns.created_start_job(dag=dag)

define_date_setup = dag_patterns.define_date_setup(
    dag=dag, default_range_ingestion=DEFAULT_RANGE_INGESTION
)

dag_patterns.define_clusters(dag=dag)

complete_process = dag_patterns.complete_process(dag=dag)

for index, file in enumerate(dag_patterns.cfg_ingestion):

    ingestion_raw = dag_patterns.raw_update_table(
        dag=dag,
        destination_table=file.RAW_TABLE,
        file=file,
        destination_table_bucket=INFO_DAG[RAW_LAYER]["BUCKET_ID"],
        destination_table_dataset=INFO_DAG[RAW_LAYER]["DATASET"],
        destination_table_project_id=PROJECT_ID,
        path_ingestion_control_partitions=INFO_DAG["INGESTION_CONTROL"][
            "PARTITIONS_FILENAME"
        ],
        path_ingestion_control_count=INFO_DAG["INGESTION_CONTROL"]["COUNT_FILENAME"],
        destination_trd_table_dataset=INFO_DAG[TRD_LAYER]["DATASET"],
        create_table_if_needed=bool(int(INFO_DAG["FLG_CREATE_TABLE_IF_NEEDED"])),
    )

    ingestion_trd = dag_patterns.trusted_update_table(
        dag=dag,
        file=file,
        destination_table=file.TRD_TABLE,
        destination_table_project_id=PROJECT_ID,
        destination_table_bucket=INFO_DAG[TRD_LAYER]["BUCKET_ID"],
        destination_table_dataset=INFO_DAG[TRD_LAYER]["DATASET"],
        origin_table_bucket=INFO_DAG[RAW_LAYER]["BUCKET_ID"],
        origin_table_dataset=INFO_DAG[RAW_LAYER]["DATASET"],
        create_table_if_needed=bool(int(INFO_DAG["FLG_CREATE_TABLE_IF_NEEDED"])),
        path_ingestion_control_partitions=INFO_DAG["INGESTION_CONTROL"][
            "PARTITIONS_FILENAME"
        ],
        path_ingestion_control_count=INFO_DAG["INGESTION_CONTROL"]["COUNT_FILENAME"],
    )

    load_data_to_bigquery = dag_patterns.load_data_to_bigquery(
        dag=dag,
        project_id=PROJECT_ID,
        bucket=INFO_DAG[TRD_LAYER]["BUCKET_ID"],
        dataset=INFO_DAG[TRD_LAYER]["DATASET"],
        table_name=file.TRD_TABLE,
        partition=file.PARTITION,
        create_if_needed=bool(int(INFO_DAG["FLG_CREATE_TABLE_IF_NEEDED"])),
    )

    data_quality = dag_patterns.data_quality(
        dag=dag,
        destination_table=file.TRD_TABLE,
        file=file,
        data_quality_info=INFO_DATA_QUALITY,
        destination_table_project_id=PROJECT_ID,
        destination_table_bucket=INFO_DAG[TRD_LAYER]["BUCKET_ID"],
        destination_table_dataset=INFO_DAG[TRD_LAYER]["DATASET"],
        path_ingestion_control_partitions=str(
            INFO_DAG["INGESTION_CONTROL"]["PARTITIONS_FILENAME"]
        ).format(
            LAYER=INFO_DAG[TRD_LAYER]["DATASET"].lower(),
            DAG_NAME=DAG_NAME.lower(),
            TABLE_NAME=file.TRD_TABLE,
        ),
        path_ingestion_control_count=str(
            INFO_DAG["INGESTION_CONTROL"]["COUNT_FILENAME"]
        ).format(
            LAYER=INFO_DAG[RAW_LAYER]["DATASET"].lower(),
            DAG_NAME=DAG_NAME.lower(),
            TABLE_NAME=file.RAW_TABLE,
        ),
    )

    (
        initial_vars
        >> define_date_setup
        >> dag_patterns.cluster.CLUSTER_CREATED
        >> ingestion_raw
        >> ingestion_trd
        >> load_data_to_bigquery
        >> data_quality
        >> dag_patterns.cluster.CLUSTER_DELETE
        >> complete_process
    )
