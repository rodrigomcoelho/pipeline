# =================================================================== #
#   This module is deprecated. Please use ./ingestion_tables_gcp.py   #
# =================================================================== #

import json
import sys
from os.path import join

from logging_patterns import LoggingPatterns
from old_ingestion_patterns_gcp import IngestionPatterns

args = IngestionPatterns.load_json_args()

logger = LoggingPatterns(
    logger="dataproc_logger",
    layer="RAW",
    airflow_context=args["CONTEXT"],
    airflow_dag_name=args["DAG_ID"],
    airflow_task_name=args["TASK_ID"],
    airflow_run_id=args["RUN_ID"],
)

logger.info("DATAPROC_LOG - START")

BIG_QUERY_TABLE_ID = f"{args['PROJECT_ID']}.{args['DATASET']}.{args['TABLE']}"

STORAGE_PATH = join("gs://", args["BUCKET"], "DATA_FILE", args["TABLE"])

logger.info(f"Starting ingestion of table `{BIG_QUERY_TABLE_ID}`")

ingestion_patterns = IngestionPatterns(
    app_name=f"ingestion-gcp-{args['TABLE']}".lower(),
    gcp_project_id=args["PROJECT_ID"],
    logger=logger,
)

range_ingestion = ingestion_patterns.define_range(
    env_range_ingestion=args["ENV_RANGE_INGESTION"],
    data_lake_tables_name=args["TABLE"],
    date_setup=args.get("DATE_SETUP"),
    dag_name=args["DAG_ID"],
)

logger.info(
    f"Range Ingestion - { json.dumps(range_ingestion, indent = 4, default = str) }"
)

formatted_query = lambda date_cast=None: ingestion_patterns.format_query(
    query=args["QUERY"],
    start_date=range_ingestion[args["TABLE"]]["START_DATE"],
    end_date=range_ingestion[args["TABLE"]]["END_DATE"],
    date_cast=date_cast,
)

df, origin_count = ingestion_patterns.read_table(
    db_type=args["DB_TYPE"],
    jdbc_url=args["URL"],
    jdbc_user=args["USER"],
    jdbc_password=args["PASSWORD"],
    formatted_query=formatted_query,
)

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

df = ingestion_patterns.normalize_columns(df=df)

partitions = ingestion_patterns.write_dataframe_gcs(
    df=df,
    destination_path=STORAGE_PATH,
    partition=args["PARTITION"],
    mode=args["MODE"],
    flg_foto=args["FLG_FOTO"],
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
