import sys
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

BIG_QUERY_TABLE_ID = f"{args['PROJECT_ID']}.{args['DATASET']}.{args['TABLE']}"

logger.info(f"Starting ingestion of table `{BIG_QUERY_TABLE_ID}`")

ingestion_patterns = IngestionPatterns(
    app_name=f"ingestion-gcp-{args['TABLE']}".lower(),
    gcp_project_id=args["PROJECT_ID"],
    logger=logger,
)

domain = args["DOMAIN"].lower()

table_trd_exists = utils.check_table_exists(
    bq_client=ingestion_patterns._bq_client,
    table_id=f"{args['PROJECT_ID']}.{args['TRD_DATASET']}.{args['TRD_TABLE']}",
)

if table_trd_exists:
    table_entry = utils.get_data_catalog_bigquery(
        dc_client=ingestion_patterns._dc_client,
        project_id=args["PROJECT_ID"],
        dataset=args["TRD_DATASET"],
        table_name=args["TRD_TABLE"],
    )
    domain = table_entry.labels.get("assunto_de_dados", domain).lower()

elif bool(args["CREATE_TABLE_IF_NEEDED"]) == False:
    msg_error = f"Table `{args['PROJECT_ID']}.{args['TRD_DATASET']}.{args['TRD_TABLE']}` is not created in bigquery."
    logger.error(msg_error)
    raise Exception(msg_error)

STORAGE_PATH = join("gs://", args["BUCKET"], domain, args["TABLE"])

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

formatted_query = lambda date_cast=None: ingestion_patterns.format_query(
    query=args["QUERY"],
    start_date=args["START_DATE"],
    end_date=args["END_DATE"],
    date_cast=date_cast,
)

credentials = ingestion_patterns.read_secret_manager(
    project_id=args["PLATFORM_PROJECT_ID"],
    secret_id=args["SECRET_ID_JDBC_CREDENTIALS"],
    deserialize_json=True,
)[args["DB_TYPE"]][args["DB_OWNER"]]

df, origin_count = ingestion_patterns.read_table(
    db_type=args["DB_TYPE"],
    jdbc_url=credentials["URL"],
    jdbc_user=credentials["USER"],
    jdbc_password=credentials["PASS"],
    formatted_query=formatted_query,
    path_log_count_records=args.get("PATH_INGESTION_CONTROL_COUNT"),
)

if origin_count < 1:
    logger.warning("The query returned no results")
    sys.exit(0)

df = ingestion_patterns.normalize_columns(df=df)

ingestion_patterns.write_dataframe_gcs(
    df=df,
    destination_path=STORAGE_PATH,
    partition=args["PARTITION"],
    mode=args["MODE"],
    flg_foto=args["FLG_FOTO"],
    path_log_written_partitions=args.get("PATH_INGESTION_CONTROL_PARTITIONS"),
)

logger.info(f"Ingestion completed successfully! [{origin_count} records]")

logger.info("DATAPROC_LOG - FINISH")
