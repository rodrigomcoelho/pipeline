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
from pyspark.sql.dataframe import DataFrame

args = IngestionPatterns.load_json_args()
args["PAYLOAD_DAG"] = ast.literal_eval(html.unescape(args["PAYLOAD_DAG"]))
dag_payload = args["PAYLOAD_DAG"]

logger = LoggingPatterns(
    logger="dataproc_logger",
    layer="AWS_HISTORIC",
    airflow_context=args["CONTEXT"],
    airflow_dag_name=args["DAG_ID"],
    airflow_task_name=args["TASK_ID"],
    airflow_run_id=args["RUN_ID"],
)

logger.info("DATAPROC_LOG - START")

ingestion_patterns = IngestionPatterns(
    app_name="ingestion-gcp-aws-historic",
    gcp_project_id=args["PROJECT_LAKE"],
    logger=logger,
)

LOAD_TYPE = dag_payload["LOAD_TYPE"]
GCP_LANDING_S3_PATH = dag_payload["GCS_S3_PATH"]
COLUMNS_TRANSFORM = (
    dag_payload["COLUMNS_TRANSFORM"] if dag_payload["COLUMNS_TRANSFORM"] != "" else "*"
)
BQ_CREATE_TABLE = args["CREATE_TABLE"]

if LOAD_TYPE == "RAW":
    GCP_STORAGE_PATH = join(args["RAW_PATH"], dag_payload["RAW"]["TABLE_NAME"])
    GCP_PARTITION_FIELD = dag_payload["RAW"]["PARTITION_FIELD"]
    GCP_CLUSTER_FIELDS = dag_payload["RAW"]["CLUSTERS_FIELDS"]
    GCP_STORAGE_PATH_TRUSTED = join(
        args["TRUSTED_PATH"], dag_payload["RAW"]["TRUSTED_TABLE_NAME"]
    )
    TABLE_ID = f"{args['PROJECT_LAKE']}.{args['TRUSTED_DATASET']}.{dag_payload['RAW']['TRUSTED_TABLE_NAME']}"
    TEMP_TABLE = dag_payload["RAW"]["TABLE_NAME"]
    FLAG_CLEAN_GCS = dag_payload["RAW"]["FLAG_CLEAN_GCS"]

    if dag_payload["RAW"].get("TABLE_NAME") == "":
        raise ValueError("RAW:TABLE_NAME => invalid")

elif LOAD_TYPE == "REFINED":
    GCP_STORAGE_PATH = join(args["REFINED_PATH"], dag_payload["REFINED"]["TABLE_NAME"])
    GCP_PARTITION_FIELD = dag_payload["REFINED"]["PARTITION_FIELD"]
    GCP_CLUSTER_FIELDS = args["REFINED_CLUSTERS_FIELDS"]
    GCP_STORAGE_PATH_TRUSTED = ""
    TABLE_ID = f"{args['PROJECT_LAKE']}.{args['REFINED_DATASET']}.{dag_payload['REFINED']['TABLE_NAME']}"
    TEMP_TABLE = args["REFINED_TABLE_NAME"]
    FLAG_CLEAN_GCS = dag_payload["REFINED"]["FLAG_CLEAN_GCS"]

    if dag_payload["REFINED"].get("TABLE_NAME") == "":
        raise ValueError("REFINED:TABLE_NAME => invalid")

logger.info(f"Starting ingestion == {LOAD_TYPE} ==> `{TABLE_ID}`")

ingestion_patterns.spark.conf.set("viewsEnabled", "true")
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
def read_parquet(storage_path: str, temp_table: str = None) -> DataFrame:
    logger.info(f"LOADING PARQUET ==>  [{storage_path}]")
    df = ingestion_patterns.spark.read.parquet(storage_path)
    df.createOrReplaceTempView(temp_table)
    return df


# ----------------------------------------------------------------------------
def exec_sql_spark(query: str) -> DataFrame:
    return ingestion_patterns.spark.sql(query)


# ----------------------------------------------------------------------------
def add_bigquery(
    table_id: str, path_parquet: str, partition: str, clusters: List, create_table: str
):
    client = bigquery.Client()

    # DROP EXTERNAL TABLE RAW (JUST USE "PARQUET")
    if LOAD_TYPE == "RAW":
        logger.info(f"Trying to drop EXTERNAL TABLE - RAW: just user PARQUET files")
        list(
            client.query(
                f"DROP TABLE IF EXISTS `{args['PROJECT_LAKE']}.{args['RAW_DATASET']}.{dag_payload['RAW']['TABLE_NAME']}`"
            )
        )

    job_config = bigquery.LoadJobConfig()

    if create_table == "Y":
        logger.info("WRITE_TRUNCATE enabled on this env....")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    else:
        logger.info("WRITE_EMPTY enabled on this env....")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_NEVER

        # TRUNCATE TABLE
        logger.info(f"Truncating table [{table_id}]...")
        list(client.query(f"TRUNCATE TABLE `{table_id}`"))

    job_config.source_format = "PARQUET"

    if partition:
        hive_config = HivePartitioningOptions()
        hive_config.mode = "AUTO"
        hive_config.source_uri_prefix = path_parquet
        job_config.hive_partitioning = hive_config

        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=getattr(bigquery.TimePartitioningType, "DAY"),
            field=partition,
            expiration_ms=None,
        )

    if clusters and clusters[0]:
        job_config.clustering_fields = clusters

    load_job = client.load_table_from_uri(
        os.path.join(path_parquet, "*"),
        table_id,
        job_config=job_config,
        project=args["PROJECT_LAKE"],
    )

    logger.info("Executing JOB LOAD...")
    load_job.result()

    destination_table = client.get_table(table_id)  # Make an API request.
    logger.info(f"Loaded {str(destination_table.num_rows)} rows.")

    log_schema = "\n".join(
        ["\t".join([row.name, row.field_type]) for row in destination_table.schema]
    )
    logger.info(f"Printing TABLE SCHEMA:\n \n{log_schema}\n \n")


# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------
logger.info("*** START AWS HISTORIC ***")

# 001 - READ PARQUET - FILES FROM S3
logger.info("001 - Reading PARQUET - Files from S3")
df_s3 = read_parquet(storage_path=GCP_LANDING_S3_PATH, temp_table=TEMP_TABLE)
logger.info("001.1 - Applying transformations...")
df_s3 = exec_sql_spark(query=f"SELECT {COLUMNS_TRANSFORM} from {TEMP_TABLE}")


# df_s3 = df_s3.where(df_s3["dat_chave"].between(F.date_sub(F.current_date(), 100), F.date_sub(F.current_date(), 0)))
df_s3.cache()
count_records = df_s3.count()

# 002 - CLEAN GCS - RAW
logger.info(
    f"002 - Cleaning GCS [FLAG_CLEAN_GCS = {FLAG_CLEAN_GCS}] ... {GCP_STORAGE_PATH}"
)
if FLAG_CLEAN_GCS == "Y":
    run_command(f"gsutil -m -q rm -r {os.path.join(GCP_STORAGE_PATH, '*')}")
    logger.info(f"002.1 - DONE")
else:
    logger.info(f"002.1 - SKIP")

# 003 - SAVING DATAFRAME
logger.info(f"003 - Saving Dataframe ... {str(count_records)} records")
df_s3.write.mode("OVERWRITE").partitionBy(GCP_PARTITION_FIELD).parquet(GCP_STORAGE_PATH)

# 003.5 - COPYING PARQUET - RAW > TRUSTED
if LOAD_TYPE == "RAW":

    logger.info(f"003.1 - Removing files from TRUSTED....")
    run_command(f"gsutil -m -q rm -r {os.path.join(GCP_STORAGE_PATH_TRUSTED, '*')}")

    logger.info(
        f"003.1 - Copying PARQUET ... RAW > TRUSTED ... {GCP_STORAGE_PATH_TRUSTED}"
    )
    run_command(
        f"gsutil -m -q cp -r {os.path.join(GCP_STORAGE_PATH, '*')} {os.path.join(GCP_STORAGE_PATH_TRUSTED, '')} "
    )

    FINAL_PATH = GCP_STORAGE_PATH_TRUSTED
else:
    FINAL_PATH = GCP_STORAGE_PATH

# 004 - ADD TO BIGQUERY
logger.info(f"004 - Adding into BigQuery....")
add_bigquery(
    table_id=TABLE_ID,
    path_parquet=FINAL_PATH,
    partition=GCP_PARTITION_FIELD,
    clusters=GCP_CLUSTER_FIELDS,
    create_table=BQ_CREATE_TABLE,
)

logger.info(f"005 - Finish ingestion AWS Historic ")
logger.info("*** FINISH ***")

logger.info("DATAPROC_LOG - FINISH")
