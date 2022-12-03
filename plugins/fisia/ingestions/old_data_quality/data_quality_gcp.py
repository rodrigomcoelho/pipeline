# ============================================================================= #
#   This module is deprecated. Please use ../data_quality/data_quality_gcp.py   #
# ============================================================================= #

import json
import sys
from datetime import datetime
from os.path import join
from urllib.parse import quote
from uuid import uuid4

from data_quality_patterns_gcp import DataQualityPatterns
from logging_patterns import LoggingPatterns
from old_ingestion_patterns_gcp import IngestionPatterns
from pytz import timezone

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

logger.info(f"Starting data quality of table `{BIG_QUERY_TABLE_ID}`")

ingestion_patterns = IngestionPatterns(
    app_name=f"ingestion-gcp-{args['TABLE']}".lower(),
    gcp_project_id=args["PROJECT_ID"],
    logger=logger,
)

count_origin = int(
    ingestion_patterns.read_file_gcs(
        ingestion_patterns.DATA_QUALITY_COUNT_STORAGE_PATH.format(
            DATAPROC_BUCKET=args["DATAPROC_BUCKET"],
            DATASET=args["DATASET"],
            DAG_ID=args["DAG_ID"],
            TABLE_NAME=args["TABLE"],
        )
    )
)

if count_origin < 1:
    logger.warning("The query returned no results")
    sys.exit(0)

partitions = json.loads(
    ingestion_patterns.read_file_gcs(
        ingestion_patterns.DATA_QUALITY_PARTITIONS_STORAGE_PATH.format(
            DATAPROC_BUCKET=args["DATAPROC_BUCKET"],
            DATASET=args["DATASET"],
            DAG_ID=args["DAG_ID"],
            TABLE_NAME=args["TABLE"],
        )
    )
)

df = ingestion_patterns.read_gcs(
    gcs_path=ingestion_patterns.get_paths_in_range_partition_gcs(
        gcs_path=STORAGE_PATH,
        partition=str(
            args["PARTITION"] or ingestion_patterns.DEFAULT_PARTITION
        ).lower(),
        partitions=partitions,
    )
)

expectations = DataQualityPatterns.get_expectations(
    table=args["TABLE"], callback_client=ingestion_patterns.execute_query_bigquery
)

if len(expectations) < 1:
    logger.warning(f"Not defined expectations for table `{args['TABLE']}`")
    sys.exit(0)

data_quality_patterns = DataQualityPatterns(df)
for row in expectations:
    getattr(data_quality_patterns, row["expectation"])(
        *json.loads(row["columns"] or "[]"),
        expectation_config=row["id_config_expectation"],
        origin_count=count_origin,
        **DataQualityPatterns.format_params(
            row["required_params"], row["submitted_params"]
        ),
    )

result_execution, _, _ = data_quality_patterns.execute_validation()

execution_id = str(uuid4())
executed_at = datetime.utcnow()
data_quality_patterns.write_results(
    spark_session=ingestion_patterns.spark,
    run_id=args["RUN_ID"],
    project_id=args["PROJECT_ID"],
    dataset=args["DATASET"],
    table=args["TABLE"],
    temp_bucket=args["DATAPROC_BUCKET"],
    data_quality_bucket=args["DATA_QUALITY_INFO"]["BUCKET_ID"],
    executed_at=executed_at,
    execution_id=execution_id,
)

data_quality_patterns.send_notifications(
    slack_token=args["DATA_QUALITY_INFO"]["TOKEN"],
    slack_ids=list(expectations[0]["slack_id"]),
    message="\n".join(args["DATA_QUALITY_INFO"]["MESSAGE"]).format(
        status=args["DATA_QUALITY_INFO"][
            "SUCCESS" if bool(result_execution[0]["flg_sucesso"]) else "FAILED"
        ],
        environment=str(args["ENVIRONMENT"]).upper(),
        layer=expectations[0]["layer"],
        table=expectations[0]["table"],
        dag=expectations[0]["dag"],
        repository=expectations[0]["repository"],
        executed_at=executed_at.astimezone(tz=timezone("America/Sao_Paulo")).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        url=args["DATA_QUALITY_INFO"]["BASE_URL"]
        + "?params="
        + quote(json.dumps(dict(env=args["PROJECT_ID"], id=execution_id))),
    ),
)

logger.info(f"Data quality of table `{BIG_QUERY_TABLE_ID}` successfully completed!")

logger.info("DATAPROC_LOG - FINISH")
