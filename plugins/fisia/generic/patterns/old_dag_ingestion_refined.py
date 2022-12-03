# ==================================================================== #
#   This module is deprecated. Please use ./dag_ingestion_refined.py   #
# ==================================================================== #

import os

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions

from .old_base_dag_patterns import BaseDagPatterns


class DagIngestionRefined(BaseDagPatterns):

    # -------------------------------------------------------------
    def __init__(
        self,
        dag_context: str,
        info_dataproc: dict,
        info_ingestion: dict,
        log_layer: str,
        log_dag_name: str,
        log_task_name: str = None,
        log_logger: str = None,
        log_identifier: str = None,
        task_timeout: float = None,
    ) -> None:
        super().__init__(
            dag_context=dag_context,
            log_identifier=log_identifier,
            log_layer=log_layer,
            log_dag_name=log_dag_name,
            log_task_name=log_task_name,
            log_logger=log_logger,
            task_timeout=task_timeout,
        )
        self.info_dataproc = info_dataproc
        self.info_ingestion = info_ingestion

    # -------------------------------------------------------------
    def define_cluster(self, dag: DAG, region: str = "US") -> None:
        self.cluster = self._define_cluster(
            dag=dag,
            db_type="INGESTION_REFINED",
            region=region,
            suffix_cluster_name=f"{region}-{self.dag_context}",
            info_dataproc=self.info_dataproc,
            info_ingestion=self.info_ingestion,
            task_id_create="create_dataproc_cluster",
            task_id_delete="delete_dataproc_cluster",
        )

    # -------------------------------------------------------------
    def update_parquet(
        self,
        dag: DAG,
        path_raw,
        path_trusted,
        bucket,
        dataset,
        destination_table,
        mode,
        bq_writedisposition,
        project,
        partition,
        type_partition,
        clustered,
        source_format,
        expiration_ms,
        flg_foto,
        workflow,
        path_refined="",
    ):
        return self._submit_job(
            dag=dag,
            task_id=f"update_parquet__{destination_table}",
            dataproc_project_id=self.cluster["DATAPROC_PROJECT_ID"],
            dataproc_cluster_name=self.cluster["DATAPROC_CLUSTER_NAME"],
            dataproc_region=self.cluster["DATAPROC_REGION"],
            main_python_file_uri=self.cluster["SPARK_JOBS"]["INGESTION"][
                "MAIN_PYTHON_FILE_URI"
            ],
            python_file_uris=self.cluster["SPARK_JOBS"]["INGESTION"][
                "PYTHON_FILE_URIS"
            ],
            CONTEXT=self.dag_context,
            PATH_RAW=path_raw,
            PATH_TRUSTED=path_trusted,
            BUCKET=bucket,
            DATASET=dataset,
            DESTINATION_TABLE=destination_table,
            MODE=mode,
            BQ_WRITEDISPOSITION=bq_writedisposition,
            PROJECT=project,
            PARTITION=partition,
            TYPE_PARTITION=type_partition,
            CLUSTERED=clustered,
            SOURCE_FORMAT=source_format,
            EXPIRATION_MS=expiration_ms,
            FLG_FOTO=flg_foto,
            WORKFLOW=workflow,
            PATH_REFINED=path_refined,
            DATAPROC_BUCKET=self.cluster["DATAPROC_BUCKET"],
        )

    # -------------------------------------------------------------
    def _write_table_bq(
        self,
        bucket,
        dataset,
        destination_table,
        project,
        partition,
        type_partition,
        clustered,
        source_format,
        expiration_ms,
    ):
        uri = os.path.join("gs://", bucket, "DATA_FILE", destination_table)
        if int(os.popen(f"gsutil ls {uri} | wc -l").read()) == 0:
            self.logging.warning("No file in directory")
            raise AirflowSkipException

        client = bigquery.Client(project=project)
        table_id = f"{project}.{dataset}.{destination_table}"

        list(client.query(f"TRUNCATE TABLE `{table_id}`"))

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_NEVER
        job_config.source_format = source_format

        if partition:
            hive_config = HivePartitioningOptions()
            hive_config.mode = "AUTO"
            hive_config.source_uri_prefix = uri
            job_config.hive_partitioning = hive_config

            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=getattr(
                    bigquery.TimePartitioningType, str(type_partition).upper() or "DAY"
                ),
                field=partition,
                expiration_ms=expiration_ms or None,
            )

        if clustered and clustered[0]:
            job_config.clustering_fields = clustered

        load_job = client.load_table_from_uri(
            os.path.join(uri, "*"), table_id, job_config=job_config, project=project
        )

        load_job.result()

    # -------------------------------------------------------------
    def write_table_bq(
        self,
        dag: DAG,
        bucket,
        dataset,
        destination_table,
        bq_writedisposition,
        project,
        partition,
        type_partition,
        clustered,
        source_format,
        expiration_ms,
    ):
        return PythonOperator(
            task_id=f"write_table_bq__{destination_table}",
            dag=dag,
            python_callable=self._write_table_bq,
            op_kwargs={
                "bucket": bucket,
                "dataset": dataset,
                "destination_table": destination_table,
                "bq_writedisposition": bq_writedisposition,
                "project": project,
                "partition": partition,
                "type_partition": type_partition,
                "clustered": clustered,
                "source_format": source_format,
                "expiration_ms": expiration_ms,
            },
        )

    # -------------------------------------------------------------
    def _generate_script(
        self, flow_file: dict, raw_path: str, trusted_path: str, destination_table: str
    ):
        log_output = "\n"
        log_output += "\n" + "from pyspark.sql import SparkSession, functions as F"
        log_output += (
            "\n"
            + "spark = SparkSession.builder.appName('automatic_script').getOrCreate()"
        )
        log_output += "\n"

        previous_df_temp = None
        for i, step in enumerate(flow_file, start=1):

            log_output += "\n" + f"## {str(i).zfill(3)} = {step['type']} .... "
            if i != len(flow_file):
                log_output += f"{step.get('temp_table')}"

            log_output += "\n" + "## " + "-" * 70
            log_output += "\n" + "## " + "-" * 70

            df_temp = f"df_temp_{str(i).zfill(3)}"

            # ----------------------------------------------
            if step["type"] == "READ_PARQUET":
                log_output += (
                    "\n"
                    + f"{df_temp} = spark.read.parquet("
                    + f"'{os.path.join(step['zone_table'].format(RAW = raw_path, TRUSTED = trusted_path), step['source_table'])}')"
                )

                if step.get("filter", {}).get("field"):
                    log_output += (
                        "\n"
                        + f"{df_temp} = {df_temp}.where("
                        + f"{df_temp}['{step['filter']['field']}'].between("
                        + f"F.date_sub(F.current_date(), {step['filter']['range_begin_from_today']}), "
                        + f"F.date_sub(F.current_date(), {step['filter']['range_end_from_today']})))"
                    )

            # ----------------------------------------------
            elif step["type"] == "READ_SQL":
                log_output += "\n" + f'{df_temp} = spark.sql("""\n{step["query"]}\n""")'

            # ----------------------------------------------
            elif step["type"] == "EXECUTE_MERGE":
                log_output += (
                    "\n"
                    + f"origin_df = spark.read.parquet("
                    + f"'{os.path.join(trusted_path, destination_table)}')"
                )

                log_output += (
                    "\n"
                    + f"{df_temp} = {previous_df_temp}.unionByName("
                    + "origin_df, allowMissingColumns = True"
                    + ").orderBy("
                    + f"{str([clause['column'] for clause in step['ordered_by']])}, "
                    + f"ascending = {str([clause['is_ascending'] for clause in step['ordered_by']])}"
                    + ").groupBy("
                    + f"*set({str(step['merge_keys'])})"
                    + ").agg("
                    + f"*[F.first(col).alias(col) for col in tuple(set(origin_df.columns) - set({str(step['merge_keys'])}))]"
                    + ")"
                )

            # ----------------------------------------------
            if step.get("temp_table"):
                log_output += (
                    "\n" + f"{df_temp}.createOrReplaceTempView('{step['temp_table']}')"
                )

            log_output += "\n"
            previous_df_temp = df_temp

        # -- FINAL
        log_output += "\n" + "## " + "-" * 70
        log_output += "\n" + "## FINAL ##"
        log_output += "\n" + "## " + "-" * 70

        log_output += "\n" + f"{df_temp}.show(100)"

        print(log_output)

    # -------------------------------------------------------------
    def generate_script(
        self,
        dag: DAG,
        flow_file: dict,
        raw_path: str,
        trusted_path: str,
        destination_table: str,
    ):
        return PythonOperator(
            task_id=f"generate_script__{destination_table}",
            dag=dag,
            python_callable=self._generate_script,
            op_kwargs={
                "flow_file": flow_file,
                "raw_path": raw_path,
                "trusted_path": trusted_path,
                "destination_table": destination_table,
            },
        )

    # -------------------------------------------------------------
    def data_quality(
        self,
        dag: DAG,
        data_quality_info: str,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        destination_table: str,
        destination_table_partition: str,
        destination_table_source_format: str,
        destination_table_flg_foto: str,
    ):
        return self._data_quality(
            dag=dag,
            task_id=f"data_quality__{destination_table}",
            spark_job=self.cluster["SPARK_JOBS"]["DATA_QUALITY"],
            dataproc_project_id=self.cluster["DATAPROC_PROJECT_ID"],
            dataproc_cluster_name=self.cluster["DATAPROC_CLUSTER_NAME"],
            dataproc_region=self.cluster["DATAPROC_REGION"],
            dataproc_bucket=self.cluster["DATAPROC_BUCKET"],
            destination_table_project_id=destination_table_project_id,
            data_quality_info=data_quality_info,
            destination_table_bucket=destination_table_bucket,
            destination_table_dataset=destination_table_dataset,
            destination_table=destination_table,
            destination_table_partition=destination_table_partition,
            destination_table_source_format=destination_table_source_format,
            destination_table_flg_foto=destination_table_flg_foto,
        )
