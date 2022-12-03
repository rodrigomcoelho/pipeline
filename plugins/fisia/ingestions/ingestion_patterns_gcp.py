import json
import logging
import math
import os
import re
import sys
from datetime import date, timedelta
from itertools import chain, zip_longest
from os.path import join
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import utils_general as utils
from adapter_dtype_bq_spark import adapter_bq_to_spark
from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.datacatalog_v1 import DataCatalogClient
from google.cloud.datacatalog_v1.types import ColumnSchema
from google.cloud.secretmanager_v1 import SecretManagerServiceClient
from logging_patterns import LoggingPatterns
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.storagelevel import StorageLevel


class IngestionPatterns:

    DEFAULT_PARTITION = "dat_chave"

    # -------------------------------------------------------------
    def __init__(
        self,
        app_name: str,
        gcp_project_id: str,
        logger: Optional[Union[logging.Logger, LoggingPatterns]] = logging,
    ) -> None:
        self.spark = (
            SparkSession.builder.config(
                "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
            )
            .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
            .appName(app_name)
            .getOrCreate()
        )

        self.logger = logger or logging
        self._bq_client = bigquery.Client(project=gcp_project_id)
        self._dc_client = DataCatalogClient()

    # -------------------------------------------------------------
    @staticmethod
    def load_json_args(arg: int = 1) -> dict:
        return json.loads(sys.argv[arg])

    # -------------------------------------------------------------
    @staticmethod
    def get_paths_in_range_partition_gcs(
        gcs_path: str,
        partition: str,
        partitions: List[str] = list(),
        max_partitions_url: int = 200,
    ) -> List[str]:
        if not partitions:
            return [join(gcs_path, "*")]

        return [
            join(gcs_path, partition + "=" + "{" + ",".join(part_partitions) + "}")
            for part_partitions in np.array_split(
                partitions, math.ceil(len(partitions) / max_partitions_url)
            )
        ]

    # -------------------------------------------------------------
    @staticmethod
    def write_file_gcs(value: str, path: str) -> None:
        os.system(f"echo '{value}' | hadoop fs -put -f - {path}")

    # -------------------------------------------------------------
    @staticmethod
    def clear_files(*files) -> None:
        for file in filter(None, files):
            IngestionPatterns.write_file_gcs("null", file)

    # -------------------------------------------------------------
    @staticmethod
    def read_file_gcs(path: str) -> str:
        return str(os.popen(f"hadoop fs -cat {path}").read())

    # -------------------------------------------------------------
    def define_storage_path(
        self,
        bucket: str,
        domain: str,
        table_name: str,
        table_needs_created: bool = True,
        search_domain_in_data_catalog: bool = True,
        data_catalog_project_id: str = None,
        data_catalog_dataset_id: str = None,
        data_catalog_table_name: str = None,
        return_table_schema: bool = False,
    ) -> Union[str, Tuple[str, Dict[str, ColumnSchema]]]:
        if table_needs_created or search_domain_in_data_catalog or return_table_schema:
            assert (
                data_catalog_project_id
                and data_catalog_dataset_id
                and data_catalog_table_name
            ), "If `table_needs_created`, `search_domain_in_data_catalog` or `return_table_schema` is required `data_catalog_project_id`, `data_catalog_dataset_id` and `data_catalog_table_name`"

            table_id = f"{data_catalog_project_id}.{data_catalog_dataset_id}.{data_catalog_table_name}"

            table_schema = None
            table_exists = utils.check_table_exists(
                bq_client=self._bq_client, table_id=table_id
            )

            assert (
                table_exists or not table_needs_created
            ), f"The table `{table_id}` is not created!"

            if table_exists and (search_domain_in_data_catalog or return_table_schema):
                table_entry = utils.get_data_catalog_bigquery(
                    dc_client=self._dc_client,
                    project_id=data_catalog_project_id,
                    dataset=data_catalog_dataset_id,
                    table_name=data_catalog_table_name,
                )

                if search_domain_in_data_catalog:
                    domain = table_entry.labels.get("assunto_de_dados", domain)

                if return_table_schema:
                    table_schema = {
                        col.column: col for col in table_entry.schema.columns
                    }

        storage_path = join("gs://", bucket, domain.lower(), table_name)

        if return_table_schema:
            return storage_path, table_schema

        return storage_path

    # -------------------------------------------------------------
    def read_secret_manager(
        self,
        project_id: str,
        secret_id: str,
        version_id: str = "latest",
        deserialize_json: bool = False,
        encoding: str = "UTF-8",
    ) -> Any:
        response = SecretManagerServiceClient().access_secret_version(
            dict(
                name=SecretManagerServiceClient.secret_version_path(
                    project_id, secret_id, version_id
                )
            )
        )

        response = response.payload.data.decode(encoding)
        if deserialize_json:
            return json.loads(response)

        return response

    # -------------------------------------------------------------
    def format_query(
        self,
        query: str,
        start_date: str = None,
        end_date: str = None,
        date_cast: str = None,
        **other_params,
    ) -> str:
        has_date_params = re.search(
            "{(START_(DATE|DATETIME))}[\s\S]*{(END_(DATE|DATETIME))}",
            query,
            re.IGNORECASE,
        )

        if not has_date_params:
            return query.format_map(other_params)

        assert (
            str(has_date_params[2]).upper() == str(has_date_params[4]).upper()
        ), f"{has_date_params[1]} is {has_date_params[2]}, but {has_date_params[3]} is {has_date_params[4]}"

        assert (
            start_date and end_date
        ), "The query has date parameters, it is necessary to provide `START_DATE` and `END_DATE`"

        start_date, end_date = date.fromisoformat(start_date), date.fromisoformat(
            end_date
        )

        assert (
            end_date >= start_date
        ), "`END_DATE` parameter must be greater than `START_DATE`"

        date_cast = date_cast or "{}"
        assert (
            date_cast.count("{}") == 1
        ), "The string `date_cast` needs to contain only one format field"

        if str(has_date_params[2]).upper() in ["DATETIME"]:
            end_date = end_date + timedelta(days=1)

        return query.format_map(
            {
                str(has_date_params[1]): date_cast.format(start_date.isoformat()),
                str(has_date_params[3]): date_cast.format(end_date.isoformat()),
                **other_params,
            }
        )

    # -------------------------------------------------------------
    def read_table(
        self,
        db_type: str,
        jdbc_url: str,
        jdbc_user: str,
        jdbc_password: str,
        formatted_query: Union[str, Callable[[str], str]],
        path_log_count_records: str = None,
    ) -> Tuple[DataFrame, int]:
        self.logger.info(f"Starting query in {db_type.upper()} database")

        formatted_query = (
            formatted_query
            if isinstance(formatted_query, Callable)
            else lambda *args, **kwargs: formatted_query
        )

        if db_type.lower() == "ifix":
            df = self.__read_ifix(
                formatted_query("TO_DATE('{}', '%Y-%m-%d')"),
                jdbc_url,
                jdbc_user,
                jdbc_password,
            )

        elif db_type.lower() == "mssql":
            df = self.__read_mssql(
                formatted_query("CONVERT(DATE, '{}', 20)"),
                jdbc_url,
                jdbc_user,
                jdbc_password,
            )

        elif db_type.lower() == "oracle":
            df = self.__read_oracle(
                formatted_query("TO_DATE('{}', 'RRRR-MM-DD')"),
                jdbc_url,
                jdbc_user,
                jdbc_password,
            )

        else:
            raise Exception(f"Database Type `{db_type}` not defined")

        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        df_count = df.count()

        if path_log_count_records:
            self.write_file_gcs(value=str(df_count), path=path_log_count_records)

        self.logger.info(f"Query in `{db_type}` finished")
        return df, df_count

    # -------------------------------------------------------------
    def read_gcs(
        self, gcs_path: Union[str, List[str]], gcs_format: str = "parquet"
    ) -> DataFrame:
        self.logger.info(f"Starting query in `Cloud Storage` -> [{gcs_path}]")
        df = self.spark.read.format(gcs_format or "parquet").load(gcs_path)
        self.logger.info("Query in `Cloud Storage` finished")
        return df

    # -------------------------------------------------------------
    def read_parquet_table(
        self,
        storage_path: str,
        temp_view_name: str = None,
        date_filter_field: str = None,
        range_begin_from_today: str = None,
        range_end_from_today: str = None,
    ) -> DataFrame:
        df = self.read_gcs(storage_path)

        if date_filter_field:
            df = df.where(
                df[date_filter_field].between(
                    F.date_sub(F.current_date(), range_begin_from_today),
                    F.date_sub(F.current_date(), range_end_from_today),
                )
            )

        if temp_view_name:
            df.createOrReplaceTempView(temp_view_name)

        return df

    # -------------------------------------------------------------
    def read_bq(self, project_id: str, dataset: str, table: str) -> DataFrame:
        self.logger.info("Starting query in `Big Query`")

        self.spark.conf.set("viewsEnabled", "true")
        self.spark.conf.set("materializationProject", project_id)
        self.spark.conf.set("materializationDataset", dataset)

        df = self.spark.read.format("bigquery").load(
            f"SELECT * FROM `{project_id}.{dataset}.{table}`"
        )

        self.logger.info("Query in `Big Query` finished")
        return df

    # -------------------------------------------------------------
    def execute_query_bigquery(self, query: str) -> pd.DataFrame:
        return self._bq_client.query(query).to_dataframe()

    # -------------------------------------------------------------
    def execute_sql_query(self, query: str, temp_view_name: str = None) -> DataFrame:
        df = self.spark.sql(query)

        if temp_view_name:
            df.createOrReplaceTempView(temp_view_name)

        return df

    # -------------------------------------------------------------
    def execute_merge(
        self,
        path_origin_table: str,
        data: DataFrame,
        partition: str,
        merge_keys: List[str],
        ordered_by: List[Dict[str, Union[str, bool]]] = [],
    ) -> DataFrame:
        merge_keys = set(merge_keys)
        origin_df = (
            self.read_gcs(path_origin_table).cache()
            if int(os.popen(f"hadoop fs -ls {path_origin_table} | wc -l").read())
            else self.spark.createDataFrame([], data.schema)
        )

        data = self.define_partition(data, partition.lower())

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

    # -------------------------------------------------------------
    def normalize_columns(self, df: DataFrame) -> DataFrame:
        return df.select([col.lower() for col in df.columns])

    # -------------------------------------------------------------
    def restructure_schema(
        self,
        df: DataFrame,
        schema: Dict[str, ColumnSchema] = None,
        format_sql: Dict[str, str] = None,
    ) -> DataFrame:
        format_sql = format_sql or dict()
        if schema and set(schema.keys()) != set(df.columns):
            self.comparative_print_schemas(
                "DATA CATALOG", schema.keys(), "DATAFRAME", df.columns
            )
            raise ValueError(
                "Columns specified in schema do not match those specified in dataframe"
            )

        columns: List[Column] = []
        for column_name in df.columns:
            column = (
                F.col(column_name)
                if column_name not in format_sql
                else F.expr(format_sql[column_name].format(col=column_name)).alias(
                    column_name
                )
            )

            columns.append(
                column.cast(adapter_bq_to_spark(schema[column_name].type))
                if schema
                else column
            )

        return df.select(*[columns])

    # -------------------------------------------------------------
    def define_partition(self, df: DataFrame, partition: str) -> DataFrame:
        partition = str(partition or self.DEFAULT_PARTITION).lower()
        return df.withColumn(
            partition,
            F.to_date(partition).cast("date")
            if partition in [col.lower() for col in df.columns]
            else F.current_date(),
        )

    # -------------------------------------------------------------
    def write_dataframe_gcs(
        self,
        df: DataFrame,
        destination_path: str,
        partition: str,
        mode: str,
        flg_foto: str,
        origin_partition: str = None,
        path_log_written_partitions: str = None,
    ) -> List[str]:
        partition = str(partition or self.DEFAULT_PARTITION).lower()

        if origin_partition:
            df = df.withColumn(partition, F.col(origin_partition.lower()))

        df = self.define_partition(df, partition)

        if flg_foto in ["1"]:
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        self.logger.info(
            f"Starting the writing of records in Cloud Storage [{destination_path}]"
        )
        df.write.mode(mode).partitionBy(partition).parquet(destination_path)
        self.logger.info(
            f"Successfully written records in Cloud Storage [{destination_path}]"
        )

        partitions = [
            row[partition].strftime("%Y-%m-%d")
            for row in df.select(partition).distinct().collect()
        ]

        if path_log_written_partitions:
            self.write_file_gcs(
                value=json.dumps(partitions), path=path_log_written_partitions
            )

        return partitions

    # -------------------------------------------------------------
    def create_table_external_hive_partitioned(
        self, table_id: str, destination_path: str
    ) -> Table:
        self.logger.info(f"Creating external table `{table_id}` in big query")

        # Example file: gs://cloud-samples-data/bigquery/hive-partitioning-samples/autolayout/dt=2020-11-15/file1.parquet
        uri = join(destination_path, "*")

        # Configure the external data source
        external_config = bigquery.ExternalConfig("PARQUET")
        external_config.source_uris = [uri]
        external_config.autodetect = True

        # Configure partitioning options
        hive_partitioning_opts = bigquery.external_config.HivePartitioningOptions()
        hive_partitioning_opts.mode = "AUTO"
        hive_partitioning_opts.require_partition_filter = False
        hive_partitioning_opts.source_uri_prefix = destination_path

        external_config.hive_partitioning = hive_partitioning_opts

        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        table = self._bq_client.create_table(table)
        self.logger.info(
            f"External table `{table.project}.{table.dataset_id}.{table.table_id}` successfully created in big query"
        )

        return table

    # -------------------------------------------------------------
    @staticmethod
    def comparative_print_schemas(
        title_schema_1: str,
        schema_1: List[str],
        title_schema_2: str,
        schema_2: List[str],
    ):
        schema_1, schema_2 = set(schema_1), set(schema_2)
        title_schema_1, title_schema_2 = (
            f" {title_schema_1.upper()} ",
            f" {title_schema_2.upper()} ",
        )
        max_field = (
            len(
                max(
                    chain(schema_1, schema_2, [title_schema_1, title_schema_2]), key=len
                )
            )
            + 4
        )

        commons = schema_1 & schema_2
        s1_only = schema_1 - schema_2
        s2_only = schema_2 - schema_1

        columns_zip = chain(
            zip_longest(s1_only, [], fillvalue=""),
            zip_longest([], s2_only, fillvalue=""),
            zip_longest(commons, commons),
        )

        print(
            title_schema_1.center(max_field, "*"),
            "|",
            title_schema_2.center(max_field, "*"),
        )
        for s1, s2 in columns_zip:
            print(s1.rjust(max_field), "|", s2.ljust(max_field))

    # -------------------------------------------------------------
    def __read_ifix(
        self, jdbc_query: str, jdbc_url: str, jdbc_user: str, jdbc_password: str
    ) -> DataFrame:
        self.logger.info(
            "Running query -> " + re.sub("(\t)|(\n)|(\s{2,})", " ", jdbc_query).strip()
        )
        return (
            self.spark.read.format("jdbc")
            .option("query", jdbc_query)
            .option("url", jdbc_url)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("fetchsize", "1000")
            .option("driver", "com.informix.jdbc.IfxDriver")
            .load()
        )

    # -------------------------------------------------------------
    def __read_mssql(
        self, jdbc_query: str, jdbc_url: str, jdbc_user: str, jdbc_password: str
    ) -> DataFrame:
        self.logger.info(
            "Running query -> " + re.sub("(\t)|(\n)|(\s{2,})", " ", jdbc_query).strip()
        )
        return (
            self.spark.read.format("jdbc")
            .option("query", jdbc_query)
            .option("url", jdbc_url)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("fetchsize", "1000")
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )

    # -------------------------------------------------------------
    def __read_oracle(
        self, jdbc_query: str, jdbc_url: str, jdbc_user: str, jdbc_password: str
    ) -> DataFrame:
        self.logger.info(
            "Running query -> " + re.sub("(\t)|(\n)|(\s{2,})", " ", jdbc_query).strip()
        )
        return (
            self.spark.read.format("jdbc")
            .option("query", jdbc_query)
            .option("url", jdbc_url)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("fetchsize", "1000")
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .option("oracle.jdbc.timezoneAsRegion", "false")
            .load()
        )
