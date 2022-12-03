# ==================================================================== #
#   This module is deprecated. Please use ./ingestion_refined_gcp.py   #
# ==================================================================== #

import json
import os
import sys
from os.path import join
from typing import Dict, List, Union

from logging_patterns import LoggingPatterns
from old_ingestion_patterns_gcp import IngestionPatterns
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

args = IngestionPatterns.load_json_args()

logger = LoggingPatterns(
    logger="dataproc_logger",
    layer="REFINED",
    airflow_context=args["CONTEXT"],
    airflow_dag_name=args["DAG_ID"],
    airflow_task_name=args["TASK_ID"],
    airflow_run_id=args["RUN_ID"],
)

BIG_QUERY_TABLE_ID = f"{args['PROJECT']}.{args['DATASET']}.{args['DESTINATION_TABLE']}"

STORAGE_PATH = join("gs://", args["BUCKET"], "DATA_FILE", args["DESTINATION_TABLE"])

logger.info("DATAPROC_LOG - START")
logger.info(f"Starting ingestion of table `{BIG_QUERY_TABLE_ID}`")

ingestion_patterns = IngestionPatterns(
    app_name=f"ingestion-gcp-{args['DESTINATION_TABLE']}".lower(),
    gcp_project_id=args["PROJECT"],
    logger=logger,
)

ingestion_patterns.spark.conf.set("viewsEnabled", "true")


def transform_parquet_temp_view(
    storage_path: str,
    source_table: str,
    temp_table: str = None,
    field: str = None,
    range_begin_from_today: str = None,
    range_end_from_today: str = None,
) -> DataFrame:
    df = ingestion_patterns.read_gcs(gcs_path=join(storage_path, source_table))

    if field:
        df = df.where(
            df[field].between(
                F.date_sub(F.current_date(), range_begin_from_today),
                F.date_sub(F.current_date(), range_end_from_today),
            )
        )

    if temp_table:
        df.createOrReplaceTempView(temp_table)

    return df


def transform_sql_temp_view(query: str, temp_table: str = None) -> DataFrame:
    df = ingestion_patterns.spark.sql(query)

    if temp_table:
        df.createOrReplaceTempView(temp_table)

    return df


def execute_merge(
    path_origin_table: str,
    data: DataFrame,
    partition: str,
    merge_keys: List[str],
    ordered_by: List[Dict[str, Union[str, bool]]] = [],
) -> DataFrame:
    merge_keys = set(merge_keys)
    origin_df = (
        ingestion_patterns.read_gcs(path_origin_table).cache()
        if int(os.popen(f"hadoop fs -ls {path_origin_table} | wc -l").read())
        else ingestion_patterns.spark.createDataFrame([], data.schema)
    )

    data = ingestion_patterns.define_partition(data, partition.lower())

    return (
        data.unionByName(origin_df, allowMissingColumns=True)
        .orderBy(
            [clause["column"] for clause in ordered_by],
            ascending=[clause.get("is_ascending", False) for clause in ordered_by],
        )
        .groupBy(*merge_keys)
        .agg(
            *[
                F.first(col).alias(col)
                for col in tuple(set(origin_df.columns) - merge_keys)
            ]
        )
    )


logger.info("Start Workflow")

for i, step in enumerate(args["WORKFLOW"], start=1):
    logger.info(f"STEP {i} - {step['type']}")

    if step["type"] == "READ_PARQUET":
        df = transform_parquet_temp_view(
            storage_path=step["zone_table"].format(
                RAW=args["PATH_RAW"],
                TRUSTED=args["PATH_TRUSTED"],
                REFINED=args["PATH_REFINED"],
            ),
            source_table=step["source_table"],
            field=step.get("filter", {}).get("field"),
            range_begin_from_today=step.get("filter", {}).get("range_begin_from_today"),
            range_end_from_today=step.get("filter", {}).get("range_end_from_today"),
            temp_table=step["temp_table"],
        )

    elif step["type"] == "READ_SQL":
        df = transform_sql_temp_view(temp_table=step["temp_table"], query=step["query"])

    elif step["type"] == "EXECUTE_MERGE":
        assert i == len(
            args["WORKFLOW"]
        ), f"The `EXECUTE_MERGE` operator is only valid in the last step ({i} of {len(args['WORKFLOW'])})"

        df = execute_merge(
            path_origin_table=STORAGE_PATH,
            data=df,
            partition=args["PARTITION"],
            merge_keys=step["merge_keys"],
            ordered_by=step.get("ordered_by", []),
        )

    else:
        raise ValueError(
            f"`{step['type']}` is not in (`READ_PARQUET`, `READ_SQL`, `EXECUTE_MERGE`)"
        )

logger.info("Finish Workflow")

origin_count = df.count()
ingestion_patterns.write_file_gcs(
    value=str(origin_count),
    path=ingestion_patterns.DATA_QUALITY_COUNT_STORAGE_PATH.format(
        DATAPROC_BUCKET=args["DATAPROC_BUCKET"],
        DATASET=args["DATASET"],
        DAG_ID=args["DAG_ID"],
        TABLE_NAME=args["DESTINATION_TABLE"],
    ),
)

if origin_count < 1:
    logger.warning("The query returned no results")
    sys.exit(0)

partitions = ingestion_patterns.write_dataframe_gcs(
    df=df,
    destination_path=STORAGE_PATH,
    partition=args["PARTITION"],
    mode=args["MODE"],
    flg_foto=args["FLG_FOTO"],
)

ingestion_patterns.write_file_gcs(
    value=str(json.dumps(partitions)),
    path=ingestion_patterns.DATA_QUALITY_PARTITIONS_STORAGE_PATH.format(
        DATAPROC_BUCKET=args["DATAPROC_BUCKET"],
        DATASET=args["DATASET"],
        DAG_ID=args["DAG_ID"],
        TABLE_NAME=args["DESTINATION_TABLE"],
    ),
)

logger.info(f"Ingestion completed successfully! [{origin_count} records]")
logger.info("DATAPROC_LOG - FINISH")
