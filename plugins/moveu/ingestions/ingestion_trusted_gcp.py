import json
import sys
from os.path import join

import utils_general as utils
from ingestion_patterns_gcp import IngestionPatterns
from logging_patterns import LoggingPatterns
from pyspark.sql import functions as F

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

BIG_QUERY_TABLE_ID = (
    f"{args['PROJECT_ID']}.{args['DESTINATION_DATASET']}.{args['DESTINATION_TABLE']}"
)

logger.info(f"Starting ingestion of table `{BIG_QUERY_TABLE_ID}`")

ingestion_patterns = IngestionPatterns(
    app_name=f"ingestion-gcp-{args['DESTINATION_TABLE']}".lower(),
    gcp_project_id=args["PROJECT_ID"],
    logger=logger,
)

domain = args["DOMAIN"].lower()
table_schema = {}

table_trd_exists = utils.check_table_exists(
    bq_client=ingestion_patterns._bq_client,
    table_id=f"{args['PROJECT_ID']}.{args['DESTINATION_DATASET']}.{args['DESTINATION_TABLE']}",
)

if table_trd_exists:
    table_entry = utils.get_data_catalog_bigquery(
        dc_client=ingestion_patterns._dc_client,
        project_id=args["PROJECT_ID"],
        dataset=args["DESTINATION_DATASET"],
        table_name=args["DESTINATION_TABLE"],
    )
    domain = table_entry.labels.get("assunto_de_dados", domain).lower()
    table_schema = {col.column: col for col in table_entry.schema.columns}

elif bool(args["CREATE_TABLE_IF_NEEDED"]) == False:
    msg_error = f"Table `{args['PROJECT_ID']}.{args['DESTINATION_DATASET']}.{args['DESTINATION_TABLE']}` is not created in bigquery."
    logger.error(msg_error)
    raise Exception(msg_error)

STORAGE_DESTINATION_PATH = join(
    "gs://", args["DESTINATION_BUCKET"], domain, args["DESTINATION_TABLE"]
)

origin_count = int(
    ingestion_patterns.read_file_gcs(
        args.get("PATH_INGESTION_CONTROL_COUNT").format(
            LAYER=args["ORIGIN_DATASET"].lower(),
            DAG_NAME=args["DAG_ID"].lower(),
            TABLE_NAME=args["ORIGIN_TABLE"],
        )
    )
)

if origin_count < 1:
    logger.warning("The query returned no results")
    sys.exit(0)

partitions = json.loads(
    ingestion_patterns.read_file_gcs(
        args.get("PATH_INGESTION_CONTROL_PARTITIONS").format(
            LAYER=args["ORIGIN_DATASET"].lower(),
            DAG_NAME=args["DAG_ID"].lower(),
            TABLE_NAME=args["ORIGIN_TABLE"],
        )
    )
)

df = ingestion_patterns.read_gcs(
    gcs_path=join("gs://", args["ORIGIN_BUCKET"], domain, args["ORIGIN_TABLE"])
).where(
    F.col(str(args["PARTITION"] or ingestion_patterns.DEFAULT_PARTITION).lower()).isin(
        *partitions
    )
)

origin_count = df.count()

ingestion_patterns.write_file_gcs(
    value=str(origin_count),
    path=args.get("PATH_INGESTION_CONTROL_COUNT").format(
        LAYER=args["DESTINATION_DATASET"].lower(),
        DAG_NAME=args["DAG_ID"].lower(),
        TABLE_NAME=args["DESTINATION_TABLE"],
    ),
)

if origin_count < 1:
    raise Exception("The query returned no results")

df = ingestion_patterns.normalize_columns(df)
df = ingestion_patterns.restructure_schema(
    df=df, schema=table_schema, format_sql=args.get("FORMAT_SQL_COLUMNS")
)

ingestion_patterns.write_dataframe_gcs(
    df=df,
    destination_path=STORAGE_DESTINATION_PATH,
    partition=args["PARTITION"],
    mode=args["MODE"] or "overwrite",
    flg_foto=args["FLG_FOTO"],
    path_log_written_partitions=args.get("PATH_INGESTION_CONTROL_PARTITIONS").format(
        LAYER=args["DESTINATION_DATASET"].lower(),
        DAG_NAME=args["DAG_ID"].lower(),
        TABLE_NAME=args["DESTINATION_TABLE"],
    ),
)

logger.info(f"Ingestion completed successfully! [{origin_count} records]")

logger.info("DATAPROC_LOG - FINISH")
