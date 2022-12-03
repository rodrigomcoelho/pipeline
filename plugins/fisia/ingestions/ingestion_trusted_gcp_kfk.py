from os.path import join

import utils_general as utils
from ingestion_patterns_gcp import IngestionPatterns
from logging_patterns import LoggingPatterns

args = IngestionPatterns.load_json_args()

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
app_name = f"ingestion-gcp-{args['DESTINATION_TABLE']}".lower()
ingestion_patterns = IngestionPatterns(
    app_name=app_name, gcp_project_id=args["PLATFORM_PROJECT_ID"], logger=logger
)
spark_session = ingestion_patterns.spark
for workflow in args["WORKFLOW"]:
    if str(workflow["TYPE"]).casefold() == str("READ_PARQUET").casefold():
        BUCKET_PATH = "gs://" + workflow["SOURCE_BUCKET"]
        logger.info(f"Reading parquet files from {BUCKET_PATH}")
        parquet_files = spark_session.read.parquet(BUCKET_PATH)
        parquet_files.createOrReplaceTempView(workflow["TEMP_TABLE"])

    if workflow["TYPE"] == "READ_SQL":
        df = spark_session.sql(workflow["QUERY"])

domain = args["DOMAIN"].lower()
table_schema = {}

table_trd_exists = utils.check_table_exists(
    bq_client=ingestion_patterns._bq_client,
    table_id=f"{args['DESTINATION_PROJECT']}.{args['DESTINATION_DATASET']}.{args['DESTINATION_TABLE']}",
)

if table_trd_exists:
    table_entry = utils.get_data_catalog_bigquery(
        dc_client=ingestion_patterns._dc_client,
        project_id=args["DESTINATION_PROJECT"],
        dataset=args["DESTINATION_DATASET"],
        table_name=args["DESTINATION_TABLE"],
    )
    domain = table_entry.labels.get("assunto_de_dados", domain).lower()
    table_schema = {col.column: col for col in table_entry.schema.columns}

elif bool(args["CREATE_TABLE_IF_NEEDED"]) == False:
    msg_error = f"Table `{args['DESTINATION_PROJECT']}.{args['DESTINATION_DATASET']}.{args['DESTINATION_TABLE']}` is not created in bigquery."
    logger.error(msg_error)
    raise Exception(msg_error)

STORAGE_DESTINATION_PATH = join(
    "gs://", args["DESTINATION_BUCKET"], domain, args["DESTINATION_TABLE"]
)

origin_count = df.count()

ingestion_patterns.write_file_gcs(
    value=str(origin_count), path=args["PATH_INGESTION_CONTROL_COUNT"]
)

if origin_count < 1:
    raise Exception("The query returned no results")

df = ingestion_patterns.normalize_columns(df)
df = ingestion_patterns.restructure_schema(
    df=df, schema=table_schema, format_sql=args.get("FORMAT_SQL_COLUMNS")
)

logger.info(f"Saving dataframe to {STORAGE_DESTINATION_PATH}")
ingestion_patterns.write_dataframe_gcs(
    df=df,
    destination_path=STORAGE_DESTINATION_PATH,
    partition=args["PARTITION"],
    mode=args["MODE"] or "overwrite",
    flg_foto=args["FLG_FOTO"],
    path_log_written_partitions=args["PATH_INGESTION_CONTROL_PARTITIONS"],
)

logger.info(f"Ingestion completed successfully! [{origin_count} records]")
logger.info("DATAPROC_LOG - FINISH")
