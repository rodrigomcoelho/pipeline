import itertools
import json
import re
import sys
from os.path import join

import utils_general as utils
from ingestion_patterns_gcp import IngestionPatterns
from logging_patterns import LoggingPatterns
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
    repository=args["LOG_REPOSITORY"],
    airflow_context=args["CONTEXT"],
    airflow_dag_name=args["DAG_ID"],
    airflow_task_name=args["TASK_ID"],
    airflow_run_id=args["RUN_ID"],
)

logger.info("DATAPROC_LOG - START")

BIG_QUERY_TABLE_ID = f"{args['LAKE_PROJECT_ID']}.{args['DATASET']}.{args['TABLE']}"

logger.info(f"Starting ingestion of table `{BIG_QUERY_TABLE_ID}`")

ingestion_patterns = IngestionPatterns(
    app_name=f"ingestion-gcp-{args['TABLE']}".lower(),
    gcp_project_id=args["LAKE_PROJECT_ID"],
    logger=logger,
)

domain = args["DOMAIN"].lower()

table_trd_exists = utils.check_table_exists(
    bq_client=ingestion_patterns._bq_client,
    table_id=f"{args['LAKE_PROJECT_ID']}.{args['TRD_DATASET']}.{args['TRD_TABLE']}",
)

if table_trd_exists:
    table_entry = utils.get_data_catalog_bigquery(
        dc_client=ingestion_patterns._dc_client,
        project_id=args["LAKE_PROJECT_ID"],
        dataset=args["TRD_DATASET"],
        table_name=args["TRD_TABLE"],
    )
    domain = table_entry.labels.get("assunto_de_dados", domain).lower()

elif bool(args["CREATE_TABLE_IF_NEEDED"]) == False:
    msg_error = f"Table `{args['LAKE_PROJECT_ID']}.{args['TRD_DATASET']}.{args['TRD_TABLE']}` is not created in bigquery."
    logger.error(msg_error)
    raise Exception()

STORAGE_PATH = join("gs://", args["DESTINATION_BUCKET"], domain, args["TABLE"])


def read_csv(
    spark_session: SparkSession,
    origin_path: str,
    infer_schema: str,
    delimiter: str = "|",
) -> DataFrame:
    return (
        spark_session.read.option("delimiter", delimiter or "|")
        .option("encoding", "UTF-8")
        .option("header", "true")
        .option("inferSchema", infer_schema)
        .csv(origin_path)
    )


def read_excel(
    spark_session: SparkSession,
    origin_path: str,
    spreadsheet_pages: list,
    range_sheets: list,
    infer_schema: str,
) -> DataFrame:
    has_pages_param = True
    if not spreadsheet_pages or spreadsheet_pages[0] in ["", "*"]:
        has_pages_param = False
    else:
        if not range_sheets or range_sheets in [None, ""]:
            range_sheets = ["A1" for i in spreadsheet_pages]

    result = spark_session.createDataFrame([], StructType([]))
    i = 0
    for _ in spreadsheet_pages if has_pages_param else itertools.count(start=0):
        try:
            if has_pages_param:
                page = spreadsheet_pages[i]
                range_sheet = range_sheets[i]
            else:
                page = i
                range_sheet = "A1"
            i += 1
            print(f"{page}!{range_sheet}")
            result = result.unionByName(
                spark_session.read.format("excel")
                .option("dataAddress", f"{page}!{range_sheet}")
                .option("header", "true")
                .option("inferSchema", infer_schema)
                .load(origin_path),
                allowMissingColumns=True,
            )
        except IllegalArgumentException as e:
            if has_pages_param:
                raise e
            else:
                break

    return result


def read_parquet(origin_path: str):
    return ingestion_patterns.read_gcs(origin_path)


def normalize_rows(df: DataFrame) -> DataFrame:
    for col in df.columns:
        if not (col.startswith("Unnamed: ") or re.search("^_c[\d]{1,}$", col)):
            expect_column_name = re.sub(
                "[^0-9a-zA-Z_]", "", unidecode(col).strip().replace(" ", "_").lower()
            )

            if col != expect_column_name:
                df = df.withColumnRenamed(col, expect_column_name)

        else:
            df = df.drop(col)

    return df.where(" OR ".join([f"{col} IS NOT NULL" for col in df.columns]))


args["PATH_INGESTION_CONTROL_COUNT"] = str(
    args.get("PATH_INGESTION_CONTROL_COUNT", "")
).format(
    LAYER=args["DATASET"].lower(),
    DAG_NAME=args["DAG_ID"].lower(),
    TABLE_NAME=args["TABLE"],
)

args["PATH_INGESTION_CONTROL_PARTITIONS"] = str(
    args.get("PATH_INGESTION_CONTROL_PARTITIONS", "")
).format(
    LAYER=args["DATASET"].lower(),
    DAG_NAME=args["DAG_ID"].lower(),
    TABLE_NAME=args["TABLE"],
)

ingestion_patterns.clear_files(
    args.get("PATH_INGESTION_CONTROL_COUNT"),
    args.get("PATH_INGESTION_CONTROL_PARTITIONS"),
)

if args["FILES"][0]["mime_type"].lower() in [
    "csv",
    "text/csv",
    "application/vnd.google-apps.spreadsheet",
]:
    df = read_csv(
        spark_session=ingestion_patterns.spark,
        origin_path=args["ORIGIN_PATH"],
        delimiter=args["DELIMITER"],
        infer_schema=str(
            args.get("INFER_SCHEMA") if args.get("INFER_SCHEMA") is not None else True
        ).lower(),
    )
elif args["FILES"][0]["mime_type"].lower() in [
    "excel",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
]:
    df = read_excel(
        spark_session=ingestion_patterns.spark,
        origin_path=args["ORIGIN_PATH"],
        spreadsheet_pages=args["SPREADSHEET_PAGES"],
        range_sheets=args["RANGE_SHEETS"],
        infer_schema=str(args.get("INFER_SCHEMA") or False).lower(),
    )
elif args["FILES"][0]["mime_type"].lower() in ["parquet"]:
    df = read_parquet(args["ORIGIN_PATH"])
else:
    raise ValueError(
        f"*** Invalid format file [{args['ORIGIN_PATH']} :: {args['FILES'][0]['mime_type']}] ***"
    )

df = normalize_rows(df=df)

if args.get("QUERY"):
    df.createOrReplaceTempView(args["TABLE"])
    formated_query = ingestion_patterns.format_query(
        args["QUERY"], args.get("START_DATE"), args.get("END_DATE"), "DATE('{}')"
    )
    df = ingestion_patterns.spark.sql(formated_query)

origin_count = df.count()
ingestion_patterns.write_file_gcs(
    value=str(origin_count), path=args.get("PATH_INGESTION_CONTROL_COUNT")
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
    path_log_written_partitions=args.get("PATH_INGESTION_CONTROL_PARTITIONS"),
)

logger.info(f"Ingestion completed successfully! [{origin_count} records]")
logger.info("DATAPROC_LOG - FINISH")
