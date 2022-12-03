# ===================================================================== #
#   This module is deprecated. Please use ./ingestion_patterns_gcp.py   #
# ===================================================================== #

import json
import logging
import math
import os
import re
import sys
from collections import ChainMap
from datetime import date, timedelta
from os.path import join
from textwrap import dedent
from typing import Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.exceptions import NotFound
from logging_patterns import LoggingPatterns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.storagelevel import StorageLevel


class IngestionPatterns:

    DEFAULT_PARTITION = "dat_chave"

    DATA_QUALITY_STORAGE_PATH = join(
        "gs://",
        "{DATAPROC_BUCKET}",
        "DATA_QUALITY",
        "logs",
        "{DATASET}",
        "{DAG_ID}",
        "{TABLE_NAME}",
    )

    DATA_QUALITY_COUNT_STORAGE_PATH = join(DATA_QUALITY_STORAGE_PATH, "COUNT.json")

    DATA_QUALITY_PARTITIONS_STORAGE_PATH = join(
        DATA_QUALITY_STORAGE_PATH, "PARTITIONS.json"
    )

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
            .config("spark.sql.session.timeZone", "America/Sao_Paulo")
            .appName(app_name)
            .getOrCreate()
        )

        self.logger = logger or logging
        self._bq_client = bigquery.Client(project=gcp_project_id)

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
    def read_file_gcs(path: str) -> str:
        return str(os.popen(f"hadoop fs -cat {path}").read())

    # -------------------------------------------------------------
    def _format_range(
        self, start_date: str = None, end_date: str = None, range_ingestion: int = None
    ) -> Tuple[bool, Dict[str, date]]:
        if start_date and end_date:
            start_date, end_date = date.fromisoformat(start_date), date.fromisoformat(
                end_date
            )
        elif range_ingestion or range_ingestion == 0:
            start_date, end_date = (
                date.today() - timedelta(days=int(range_ingestion)),
                date.today(),
            )
        else:
            return False, None

        assert (
            end_date >= start_date
        ), "`END_DATE` parameter must be greater than `START_DATE`"

        return True, dict(START_DATE=start_date, END_DATE=end_date)

    # -------------------------------------------------------------
    def _format_range_by_tables(
        self,
        tables_name: List[str],
        setup_by_table: Dict[str, Dict[str, str]],
    ) -> Tuple[Dict[str, Dict[str, date]], List[str]]:
        tables_defined, tables_not_defined = dict(), list()
        setup = setup_by_table.get("*", dict())
        default_setup = self._format_range(
            start_date=setup.get("START_DATE"),
            end_date=setup.get("END_DATE"),
            range_ingestion=setup.get("RANGE_INGESTION"),
        )

        for table_name in tables_name:
            setup = setup_by_table.get(table_name)
            is_success, table_range = (
                default_setup
                if not setup
                else self._format_range(
                    setup.get("START_DATE"),
                    setup.get("END_DATE"),
                    setup.get("RANGE_INGESTION"),
                )
            )

            if is_success:
                tables_defined[table_name] = table_range
            else:
                tables_not_defined.append(table_name)

        return tables_defined, tables_not_defined

    # -------------------------------------------------------------
    def _get_setup_date_bq(
        self,
        data_lake_tables_name: List[str],
        dag_name: str = None,
        data_lake_dependent_table_name: str = None,
    ) -> Dict[str, Dict[str, str]]:
        params = [
            dict(
                nom_dag=dag_name,
                nom_tabela_dependente=data_lake_dependent_table_name,
                nom_tabela=table,
            )
            for table in data_lake_tables_name
        ]

        query = dedent(
            f"""
                WITH tables_selected AS (
                    SELECT
                        CAST(JSON_EXTRACT_SCALAR(tabs, '$.nom_dag') AS STRING) AS nom_dag,
                        CAST(JSON_EXTRACT_SCALAR(tabs, '$.nom_tabela_dependente') AS STRING) AS nom_tabela_dependente,
                        CAST(JSON_EXTRACT_SCALAR(tabs, '$.nom_tabela') AS STRING) AS nom_tabela
                    FROM
                        UNNEST(JSON_EXTRACT_ARRAY('{ json.dumps(params) }')) tabs
                ),
                tables_range AS (
                    SELECT
                        B.nom_tabela,
                        A.dat_inicio,
                        A.dat_fim,
                        A.num_dias_carga,
                        ROW_NUMBER() OVER(
                            PARTITION BY
                                B.nom_tabela
                            ORDER BY
                                A.nom_tabela ASC NULLS LAST,
                                A.nom_tabela_dependente ASC NULLS LAST,
                                A.nom_dag ASC NULLS LAST
                        ) AS position
                    FROM
                        `ingestion_control.cnt_tab_config_ingestao` A
                    RIGHT JOIN
                        tables_selected B
                    ON
                        (
                            A.nom_dag = B.nom_dag
                            AND
                            A.nom_tabela_dependente IS NULL
                            AND
                            A.nom_tabela IS NULL
                        ) OR (
                            A.nom_tabela_dependente = B.nom_tabela_dependente
                            AND
                            A.nom_tabela IS NULL
                            AND
                            (COALESCE(A.nom_dag, 'NO_VALUE') IN (B.nom_dag, 'NO_VALUE'))
                        ) OR (
                            A.nom_tabela = B.nom_tabela
                            AND
                            (COALESCE(A.nom_dag, 'NO_VALUE') IN (B.nom_dag, 'NO_VALUE'))
                            AND
                            (COALESCE(A.nom_tabela_dependente, 'NO_VALUE') = COALESCE(B.nom_tabela_dependente, 'NO_VALUE'))
                        )
                )
                SELECT
                    CAST(nom_tabela AS STRING) AS NOM_TABELA,
                    CAST(dat_inicio AS STRING) AS START_DATE,
                    CAST(dat_fim AS STRING) AS END_DATE,
                    CAST(num_dias_carga AS STRING) AS RANGE_INGESTION
                FROM
                    tables_range
                WHERE
                    position = 1
            """
        )

        return {
            row["NOM_TABELA"]: row[
                ["START_DATE", "END_DATE", "RANGE_INGESTION"]
            ].to_dict()
            for _, row in self.execute_query_bigquery(query).iterrows()
        }

    # -------------------------------------------------------------
    def define_range(
        self,
        env_range_ingestion: int,
        data_lake_tables_name: Union[str, List[str]],
        date_setup: dict = dict(),
        dag_name: str = None,
        data_lake_dependent_table_name: str = None,
    ) -> Dict[str, Dict[str, date]]:
        tables_range = ChainMap()
        data_lake_tables_name = tables_not_defined = (
            [data_lake_tables_name]
            if isinstance(data_lake_tables_name, str)
            else list(data_lake_tables_name)
        )

        if date_setup:
            tables_defined, tables_not_defined = self._format_range_by_tables(
                tables_name=tables_not_defined, setup_by_table={"*": date_setup}
            )
            tables_range.maps.append(tables_defined or dict())

        if tables_not_defined and data_lake_tables_name:
            tables_defined, tables_not_defined = self._format_range_by_tables(
                tables_name=tables_not_defined,
                setup_by_table=self._get_setup_date_bq(
                    data_lake_tables_name=data_lake_tables_name,
                    dag_name=dag_name,
                    data_lake_dependent_table_name=data_lake_dependent_table_name,
                ),
            )
            tables_range.maps.append(tables_defined or dict())

        if tables_not_defined:
            tables_defined, tables_not_defined = self._format_range_by_tables(
                tables_name=tables_not_defined,
                setup_by_table={"*": {"RANGE_INGESTION": env_range_ingestion}},
            )
            tables_range.maps.append(tables_defined or dict())

        return dict(tables_range)

    # -------------------------------------------------------------
    def format_query(
        self,
        query: str,
        start_date: date = None,
        end_date: date = None,
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
    def normalize_columns(self, df: DataFrame) -> DataFrame:
        return df.select([col.lower() for col in df.columns])

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

        return [
            row[partition].strftime("%Y-%m-%d")
            for row in df.select(partition).distinct().collect()
        ]

    # -------------------------------------------------------------
    def check_table_exists(self, table_id: str) -> bool:
        try:
            return bool(self._bq_client.get_table(table_id))
        except NotFound:
            return False

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
