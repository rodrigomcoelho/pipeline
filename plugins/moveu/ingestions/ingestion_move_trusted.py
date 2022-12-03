import ast
import html
import os
import subprocess
from os.path import join
from typing import List

from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions
from logging_patterns import LoggingPatterns
from old_ingestion_patterns_gcp import IngestionPatterns

args = IngestionPatterns.load_json_args()
args["PAYLOAD_DAG"] = ast.literal_eval(html.unescape(args["PAYLOAD_DAG"]))
dag_payload = args["PAYLOAD_DAG"]
list_tables = dag_payload["TABLES"]

logger = LoggingPatterns(
    logger="dataproc_logger",
    layer="MOVE_TRUSTED",
    airflow_context=args["CONTEXT"],
    airflow_dag_name=args["DAG_ID"],
    airflow_task_name=args["TASK_ID"],
    airflow_run_id=args["RUN_ID"],
)

logger.info("DATAPROC_LOG - START")

ingestion_patterns = IngestionPatterns(
    app_name="ingestion-gcp-move-trusted",
    gcp_project_id=args["PROJECT_LAKE"],
    logger=logger,
)

ingestion_patterns.spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
ingestion_patterns.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
ingestion_patterns.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
ingestion_patterns.spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
ingestion_patterns.spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
ingestion_patterns.spark.conf.set("spark.sql.execution.arrow.enabled", "true")
ingestion_patterns.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
ingestion_patterns.spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
ingestion_patterns.spark.conf.set(
    "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
)
ingestion_patterns.spark.conf.set(
    "spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED"
)
ingestion_patterns.spark.conf.set(
    "spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED"
)
ingestion_patterns.spark.conf.set(
    "spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED"
)
ingestion_patterns.spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

# ----------------------------------------------------------------------------
def run_command(command: str):
    subprocess.Popen(
        command, stdout=subprocess.PIPE, executable="/bin/bash", shell=True
    ).wait()


# ----------------------------------------------------------------------------
def add_bigquery(
    raw_table_id: str,
    trusted_table_id: str,
    trusted_path_parquet: str,
    partition: str,
    clusters: List,
    create_table: str,
):
    client = bigquery.Client()

    logger.info(f"Trying to drop EXTERNAL TABLE - RAW: just user PARQUET files")
    list(client.query(f"DROP TABLE IF EXISTS `{raw_table_id}`"))

    job_config = bigquery.LoadJobConfig()

    if create_table == "Y":
        logger.info("WRITE_TRUNCATE enabled on this env....")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    else:
        logger.info("WRITE_EMPTY enabled on this env....")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_NEVER

        # TRUNCATE TABLE
        logger.info(f"Truncating table [{trusted_table_id}]...")
        list(client.query(f"TRUNCATE TABLE `{trusted_table_id}`"))

    job_config.source_format = "PARQUET"

    if partition:
        hive_config = HivePartitioningOptions()
        hive_config.mode = "AUTO"
        hive_config.source_uri_prefix = trusted_path_parquet
        job_config.hive_partitioning = hive_config

        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=getattr(bigquery.TimePartitioningType, "DAY"),
            field=partition,
            expiration_ms=None,
        )

    if clusters and clusters[0]:
        job_config.clustering_fields = clusters

    load_job = client.load_table_from_uri(
        os.path.join(trusted_path_parquet, "*"),
        trusted_table_id,
        job_config=job_config,
        project=args["PROJECT_LAKE"],
    )

    logger.info("Executing JOB LOAD...")
    load_job.result()

    destination_table = client.get_table(trusted_table_id)  # Make an API request.
    logger.info(f"Loaded {str(destination_table.num_rows)} rows.")

    log_schema = "\n".join(
        ["\t".join([row.name, row.field_type]) for row in destination_table.schema]
    )
    logger.info(f"Printing TABLE SCHEMA:\n \n{log_schema}\n \n")


# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

logger.info("*** START - MOVE TRUSTED ***")
qty = len(list_tables)

if qty <= 0:
    raise ValueError("<TABLES> invalid.")

for i, table_info in enumerate(list_tables, 1):
    logger.info("=" * 50)
    logger.info(f'({i}/{qty}) Starting table >> {table_info["RAW_TABLE_NAME"]}')

    RAW_TABLE_ID = (
        f"{args['PROJECT_LAKE']}.{args['RAW_DATASET']}.{table_info['RAW_TABLE_NAME']}"
    )
    GCP_STORAGE_PATH_RAW = join(args["RAW_PATH"], table_info["RAW_TABLE_NAME"])
    TRUSTED_TABLE_ID = f"{args['PROJECT_LAKE']}.{args['TRUSTED_DATASET']}.{table_info['TRUSTED_TABLE_NAME']}"
    GCP_STORAGE_PATH_TRUSTED = join(
        args["TRUSTED_PATH"], table_info["TRUSTED_TABLE_NAME"]
    )
    GCP_PARTITION_FIELD = table_info["PARTITION_FIELD"]
    GCP_CLUSTER_FIELDS = table_info["CLUSTERS_FIELDS"]
    BQ_CREATE_TABLE = args["CREATE_TABLE"]

    if not table_info["RAW_TABLE_NAME"]:
        raise ValueError("RAW_TABLE_NAME => invalid")

    if not table_info["TRUSTED_TABLE_NAME"]:
        raise ValueError("TRUSTED_TABLE_NAME => invalid")

    logger.info(f"RAW_PATH => {GCP_STORAGE_PATH_RAW}")
    logger.info(f"TRUSTED_PATH => {GCP_STORAGE_PATH_TRUSTED}")

    # 001 - CLEAN TRUSTED PATH
    logger.info(f"001 - Cleaning path - TRUSTED >> {GCP_STORAGE_PATH_TRUSTED}")
    run_command(f"gsutil -m -q rm -r {os.path.join(GCP_STORAGE_PATH_TRUSTED, '*')}")

    # 002 - COPY PARQUETS - RAW > TRUSTED
    logger.info(f"002 - Copying PARQUETs - RAW > TRUSTED")
    run_command(
        f"gsutil -m -q cp -r {os.path.join(GCP_STORAGE_PATH_RAW, '*')} {os.path.join(GCP_STORAGE_PATH_TRUSTED, '')} "
    )

    # 003 - ADD TO BIGQUERY
    logger.info(f"004 - Adding into BigQuery....")
    add_bigquery(
        raw_table_id=RAW_TABLE_ID,
        trusted_table_id=TRUSTED_TABLE_ID,
        trusted_path_parquet=GCP_STORAGE_PATH_TRUSTED,
        partition=GCP_PARTITION_FIELD,
        clusters=GCP_CLUSTER_FIELDS,
        create_table=BQ_CREATE_TABLE,
    )
    logger.info(f'Done - [{table_info["RAW_TABLE_NAME"]}]')

logger.info("*** FINISH ***")

logger.info("DATAPROC_LOG - FINISH")
