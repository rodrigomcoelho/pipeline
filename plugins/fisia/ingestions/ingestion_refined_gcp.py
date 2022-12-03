import sys

from ingestion_patterns_gcp import IngestionPatterns
from logging_patterns import LoggingPatterns

args = IngestionPatterns.load_json_args()

logger = LoggingPatterns(
    logger="dataproc_logger",
    layer="REFINED",
    repository=args["LOG_REPOSITORY"],
    airflow_context=args["CONTEXT"],
    airflow_dag_name=args["DAG_ID"],
    airflow_task_name=args["TASK_ID"],
    airflow_run_id=args["RUN_ID"],
)

logger.info("DATAPROC_LOG - START")

BIG_QUERY_TABLE_ID = f"{args['DESTINATION_PROJECT']}.{args['DESTINATION_DATASET']}.{args['DESTINATION_TABLE']}"
logger.info(f"Starting ingestion of table `{BIG_QUERY_TABLE_ID}`")

ingestion_patterns = IngestionPatterns(
    app_name=f"ingestion-gcp-{args['DESTINATION_TABLE']}".lower(),
    gcp_project_id=args["DESTINATION_PROJECT"],
    logger=logger,
)

STORAGE_PATH, TABLE_SCHEMA = ingestion_patterns.define_storage_path(
    bucket=args["DESTINATION_BUCKET"],
    domain=args["DOMAIN"],
    table_name=args["DESTINATION_TABLE"],
    table_needs_created=not bool(args["CREATE_TABLE_IF_NEEDED"]),
    data_catalog_project_id=args["DESTINATION_PROJECT"],
    data_catalog_dataset_id=args["DESTINATION_DATASET"],
    data_catalog_table_name=args["DESTINATION_TABLE"],
    return_table_schema=True,
)

logger.info("Start Workflow")
for i, step in enumerate(args["WORKFLOW"], start=1):
    logger.info(f"STEP {i} - {step['TYPE']}")

    if str(step["TYPE"]).casefold() == str("READ_PARQUET").casefold():
        date_filter = step.get("FILTER") or dict()
        df = ingestion_patterns.read_parquet_table(
            storage_path=ingestion_patterns.define_storage_path(
                bucket=args["ENVIRONMENT_SETUP"][step["BU"]][step["LAYER"]][
                    "BUCKET_ID"
                ],
                domain=None,
                table_name=step["SOURCE_TABLE"],
                data_catalog_project_id=args["ENVIRONMENT_SETUP"][step["BU"]][
                    "PROJECT_ID"
                ],
                data_catalog_dataset_id=args["ENVIRONMENT_SETUP"][step["BU"]][
                    step["LAYER"]
                ]["DATASET"],
                data_catalog_table_name=step["SOURCE_TABLE"],
            ),
            temp_view_name=step.get("TEMP_TABLE"),
            date_filter_field=date_filter.get("FIELD"),
            range_begin_from_today=date_filter.get("RANGE_BEGIN_FROM_TODAY"),
            range_end_from_today=date_filter.get("RANGE_END_FROM_TODAY"),
        )

    elif str(step["TYPE"]).casefold() == str("READ_SQL").casefold():
        df = ingestion_patterns.execute_sql_query(
            query=step["QUERY"], temp_view_name=step.get("TEMP_TABLE")
        )

    elif str(step["TYPE"]).casefold() == str("EXECUTE_MERGE").casefold():
        assert i == len(
            args["WORKFLOW"]
        ), f"The `EXECUTE_MERGE` operator is only valid in the last step ({i} of {len(args['WORKFLOW'])})"

        df = ingestion_patterns.execute_merge(
            path_origin_table=STORAGE_PATH,
            data=df,
            partition=args["PARTITION"],
            merge_keys=step["MERGE_KEYS"],
            ordered_by=step.get("ORDERED_BY", []),
        )

    else:
        raise ValueError(
            f"`{step['TYPE']}` is not in (`READ_PARQUET`, `READ_SQL`, `EXECUTE_MERGE`)"
        )

logger.info("Finish Workflow")

origin_count = df.count()
ingestion_patterns.write_file_gcs(
    value=str(origin_count), path=args["PATH_INGESTION_CONTROL_COUNT"]
)

if origin_count < 1:
    logger.warning("The query returned no results")
    sys.exit(0)

df = ingestion_patterns.normalize_columns(df=df)

df = ingestion_patterns.restructure_schema(df=df, schema=TABLE_SCHEMA)

ingestion_patterns.write_dataframe_gcs(
    df=df,
    destination_path=STORAGE_PATH,
    partition=args["PARTITION"],
    mode=args["MODE"],
    flg_foto=args["FLG_FOTO"],
    path_log_written_partitions=args["PATH_INGESTION_CONTROL_PARTITIONS"],
)

logger.info(f"Ingestion completed successfully! [{origin_count} records]")
logger.info("DATAPROC_LOG - FINISH")
