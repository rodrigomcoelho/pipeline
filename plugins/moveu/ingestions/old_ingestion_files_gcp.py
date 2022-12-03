# ================================================================== #
#   This module is deprecated. Please use ./ingestion_files_gcp.py   #
# ================================================================== #

import itertools
import json
import re
import sys
from os.path import join

from logging_patterns import LoggingPatterns
from old_ingestion_patterns_gcp import IngestionPatterns
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import IllegalArgumentException
from unidecode import unidecode

args = IngestionPatterns.load_json_args()

if isinstance(args["FILES"], str):
    args["FILES"] = json.loads(args["FILES"].replace("'", '"'))

logger = LoggingPatterns(
    logger="dataproc_logger",
    layer="RAW",
    airflow_context=args["CONTEXT"],
    airflow_dag_name=args["DAG_ID"],
    airflow_task_name=args["TASK_ID"],
    airflow_run_id=args["RUN_ID"],
)

logger.info("DATAPROC_LOG - START")

BIG_QUERY_TABLE_ID = f"{args['LAKE_PROJECT_ID']}.{args['DATASET']}.{args['TABLE']}"

STORAGE_PATH = join("gs://", args["DESTINATION_BUCKET"], "DATA_FILE", args["TABLE"])

logger.info(f"Starting ingestion of table `{BIG_QUERY_TABLE_ID}`")

ingestion_patterns = IngestionPatterns(
    app_name=f"ingestion-gcp-{args['TABLE']}".lower(),
    gcp_project_id=args["LAKE_PROJECT_ID"],
    logger=logger,
)


def read_csv(
    spark_session: SparkSession, origin_path: str, delimiter: str = "|"
) -> DataFrame:
    return (
        spark_session.read.option("delimiter", delimiter or "|")
        .option("encoding", "UTF-8")
        .option("header", "True")
        .option("inferSchema", "true")
        .csv(origin_path)
    )


def read_excel(
    spark_session: SparkSession, origin_path: str, spreadsheet_pages: list
) -> DataFrame:
    has_pages_param = True
    if not spreadsheet_pages or spreadsheet_pages[0] in ["", "*"]:
        has_pages_param = False

    result = spark_session.createDataFrame([], StructType([]))
    for page in spreadsheet_pages if has_pages_param else itertools.count(start=0):
        try:
            result = result.unionByName(
                spark_session.read.format("excel")
                .option("dataAddress", f"{page}!A1")
                .option("header", "true")
                .option("inferSchema", "false")
                .load(origin_path),
                allowMissingColumns=True,
            )
        except IllegalArgumentException as e:
            if has_pages_param:
                raise e
            else:
                break

    return result


def normalize_rows(df: DataFrame) -> DataFrame:
    for col in df.columns:
        if not (col.startswith("Unnamed: ") or re.search("^_c[\d]{1,}$", col)):
            df = df.withColumnRenamed(
                col,
                re.sub(
                    "[^0-9a-zA-Z_]",
                    "",
                    unidecode(col).strip().replace(" ", "_").lower(),
                ),
            )
        else:
            df = df.drop(col)

    return df.where(" OR ".join([f"{col} IS NOT NULL" for col in df.columns]))


if args["FILES"][0]["mime_type"].lower() in [
    "csv",
    "text/csv",
    "application/vnd.google-apps.spreadsheet",
]:
    df = read_csv(ingestion_patterns.spark, args["ORIGIN_PATH"], args["DELIMITER"])
elif args["FILES"][0]["mime_type"].lower() in [
    "excel",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
]:
    df = read_excel(
        ingestion_patterns.spark, args["ORIGIN_PATH"], args["SPREADSHEET_PAGES"]
    )
else:
    raise ValueError(
        f"*** Invalid format file [{args['ORIGIN_PATH']} :: {args['FILES'][0]['mime_type']}] ***"
    )

df = normalize_rows(df=df)

origin_count = df.count()
ingestion_patterns.write_file_gcs(
    value=str(origin_count),
    path=ingestion_patterns.DATA_QUALITY_COUNT_STORAGE_PATH.format(
        DATAPROC_BUCKET=args["DATAPROC_BUCKET"],
        DATASET=args["DATASET"],
        DAG_ID=args["DAG_ID"],
        TABLE_NAME=args["TABLE"],
    ),
)

if origin_count < 1:
    logger.warning("The query returned no results")
    sys.exit(0)

partitions = ingestion_patterns.write_dataframe_gcs(
    df=df,
    destination_path=STORAGE_PATH,
    partition=args.get("PARTITION") or ingestion_patterns.DEFAULT_PARTITION,
    origin_partition=args.get("ORIGIN_PARTITION"),
    mode=args.get("MODE") or "overwrite",
    flg_foto=args.get("FLG_FOTO") or "0",
)

ingestion_patterns.write_file_gcs(
    value=json.dumps(partitions),
    path=ingestion_patterns.DATA_QUALITY_PARTITIONS_STORAGE_PATH.format(
        DATAPROC_BUCKET=args["DATAPROC_BUCKET"],
        DATASET=args["DATASET"],
        DAG_ID=args["DAG_ID"],
        TABLE_NAME=args["TABLE"],
    ),
)

if ingestion_patterns.check_table_exists(BIG_QUERY_TABLE_ID) is False:
    ingestion_patterns.create_table_external_hive_partitioned(
        table_id=BIG_QUERY_TABLE_ID, destination_path=STORAGE_PATH
    )

logger.info(f"Ingestion completed successfully! [{origin_count} records]")
logger.info("DATAPROC_LOG - FINISH")
