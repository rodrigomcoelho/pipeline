import json
import sys
from datetime import datetime
from os.path import join
from urllib.parse import quote
from uuid import uuid4

import utils_general as utils
from data_quality_patterns_gcp import DataQualityPatterns
from google.cloud import bigquery
from google.cloud.datacatalog_v1 import DataCatalogClient
from ingestion_patterns_gcp import IngestionPatterns
from logging_patterns import LoggingPatterns
from pytz import timezone

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

bq_client = bigquery.Client(project=args["PROJECT_ID"])
domain = args["DOMAIN"].lower()

table_trd_exists = utils.check_table_exists(
    bq_client=bq_client,
    table_id=f"{args['PROJECT_ID']}.{args['DATASET']}.{args['TABLE']}",
)

if table_trd_exists:
    dc_client = DataCatalogClient()
    table_entry = utils.get_data_catalog_bigquery(
        dc_client=dc_client,
        project_id=args["PROJECT_ID"],
        dataset=args["DATASET"],
        table_name=args["TABLE"],
    )
    domain = table_entry.labels.get("assunto_de_dados", domain).lower()

STORAGE_PATH = join("gs://", args["BUCKET"], domain, args["TABLE"])

logger.info(f"Starting data quality of table `{BIG_QUERY_TABLE_ID}`")

ingestion_patterns = IngestionPatterns(
    app_name=f"ingestion-gcp-{args['TABLE']}".lower(),
    gcp_project_id=args["PROJECT_ID"],
    logger=logger,
)

count_origin = int(
    ingestion_patterns.read_file_gcs(args.get("PATH_INGESTION_CONTROL_COUNT"))
)

if count_origin < 1:
    logger.warning("The query returned no results")
    sys.exit(0)

expectations = DataQualityPatterns.get_expectations(
    table=args["TABLE"],
    callback_client=ingestion_patterns.execute_query_bigquery,
    expectations_project=args["DATA_QUALITY_INFO"]["DATA_QUALITY_PROJECT_ID"],
)

if len(expectations) < 1:
    logger.warning(f"Not defined expectations for table `{args['TABLE']}`")
    sys.exit(0)

partitions = json.loads(
    ingestion_patterns.read_file_gcs(args.get("PATH_INGESTION_CONTROL_PARTITIONS"))
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
    project_id=args["DATA_QUALITY_INFO"]["DATA_QUALITY_PROJECT_ID"],
    dataset=args["DATASET"],
    table=args["TABLE"],
    temp_bucket=args["DATAPROC_BUCKET"],
    data_quality_bucket=args["DATA_QUALITY_INFO"]["DATA_QUALITY_BUCKET_ID"],
    bu_code=args["DATA_QUALITY_INFO"]["BU_CODE"],
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
