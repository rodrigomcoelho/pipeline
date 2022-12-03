from typing import Any, Dict, List

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from ..templates import versioning
from ..types.cluster import SparkJob
from ..types.trusted.cfg_ingestion_kfk import IngestionKfk
from .base_dag_patterns import BaseDagPatterns


class DagIngestionTrusted(BaseDagPatterns):

    # Versão do template de criação das DAGs
    TEMPLATE_VERSION: float = versioning.TRD_INGESTION_KFK

    # -------------------------------------------------------------
    def __init__(
        self,
        script_version: float,
        layer: str,
        domain: str,
        dag_name: str,
        dag_context: str,
        info_dataproc: dict,
        path_ingestion_config: str,
        info_ingestion: dict,
        info_quality_ingestion: dict = None,
        log_task_name: str = None,
        log_identifier: str = None,
        log_logger: str = None,
        log_run_id: str = None,
        log_repository: str = None,
        task_timeout: float = None,
    ) -> None:
        super().__init__(
            script_version=script_version,
            dag_context=dag_context,
            layer=layer,
            domain=domain,
            dag_name=dag_name,
            log_identifier=log_identifier,
            log_run_id=log_run_id,
            log_task_name=log_task_name,
            log_logger=log_logger,
            task_timeout=task_timeout,
            log_repository=log_repository,
        )

        self.info_dataproc = info_dataproc
        self.info_ingestion = info_ingestion
        self.info_quality_ingestion = info_quality_ingestion
        self.cfg_ingestion = self._adapter_result(
            self._load_ingestion_file(path_ingestion_config)
        )

    # -------------------------------------------------------------
    def _adapter_result(self, content_file: Dict[str, Any]) -> List[IngestionKfk]:
        if content_file["ACTIVE"] == "1":
            return [IngestionKfk(**content_file)]

        return []

    # -------------------------------------------------------------
    def define_spark_jobs(self, spark_job: dict, data_quality_spark_job: dict):
        self.spark_job = SparkJob(**spark_job)
        self.data_quality_spark_job = SparkJob(**data_quality_spark_job)

    # -------------------------------------------------------------
    def define_cluster(self, dag: DAG, region: str = "US") -> None:
        self.cluster = self._define_cluster(
            dag=dag,
            db_type="INGESTION_TRUSTED",
            region=region,
            tables=[cfg_table.TRD_TABLE for cfg_table in self.cfg_ingestion],
            suffix_cluster_name=f"{region}-{self.dag_context}",
            info_dataproc=self.info_dataproc,
            task_id_create="create_dataproc_cluster",
            task_id_delete="delete_dataproc_cluster",
        )

    # -------------------------------------------------------------
    def consolidate_origins(self, pattern_variables: str) -> Dict[str, Dict[str, Any]]:
        collection_bu = {
            workflow.BU.upper()
            for cfg_table in self.cfg_ingestion
            for workflow in cfg_table.WORKFLOW
            if workflow.SOURCE_TABLE
        }

        environment_setup = dict()
        for bu in collection_bu:
            environment_setup[bu] = self.get_custom_variable(
                pattern_variables.format(bu.upper())
            )

        return environment_setup

    # -------------------------------------------------------------
    def update_table(
        self,
        dag: DAG,
        destination_table: str,
        cfg_table: IngestionKfk,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        create_table_if_needed: bool,
        environment_setup: Dict[str, Dict[str, Any]],
        path_ingestion_control_partitions: str,
        path_ingestion_control_count: str,
    ):
        return self._ingestion(
            dag=dag,
            task_id=f"trd_update_table__{destination_table}",
            spark_job=self.spark_job,
            dataproc_project_id=self.cluster.DATAPROC_PROJECT_ID,
            dataproc_cluster_name=self.cluster.DATAPROC_CLUSTER_NAME,
            dataproc_region=self.cluster.DATAPROC_REGION,
            dataproc_bucket=self.cluster.DATAPROC_BUCKET,
            DESTINATION_PROJECT=destination_table_project_id,
            DESTINATION_BUCKET=destination_table_bucket,
            DESTINATION_DATASET=destination_table_dataset,
            DESTINATION_TABLE=cfg_table.TRD_TABLE,
            MODE=cfg_table.MODE,
            PARTITION=cfg_table.PARTITION,
            TYPE_PARTITION=cfg_table.TYPE_PARTITION,
            CLUSTERED=cfg_table.CLUSTERED,
            SOURCE_FORMAT=cfg_table.SOURCE_FORMAT,
            FLG_FOTO=cfg_table.FLG_FOTO,
            WORKFLOW=[workflow.to_dict() for workflow in cfg_table.WORKFLOW],
            CREATE_TABLE_IF_NEEDED=create_table_if_needed,
            ENVIRONMENT_SETUP=environment_setup,
            PATH_INGESTION_CONTROL_PARTITIONS=path_ingestion_control_partitions,
            PATH_INGESTION_CONTROL_COUNT=path_ingestion_control_count,
        )

    # -------------------------------------------------------------
    def data_quality(
        self,
        dag: DAG,
        destination_table: str,
        cfg_table: IngestionKfk,
        data_quality_info: dict,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        path_ingestion_control_partitions: str,
        path_ingestion_control_count: str,
    ):
        return self._data_quality(
            dag=dag,
            task_id=f"data_quality_{destination_table}",
            spark_job=self.data_quality_spark_job,
            dataproc_project_id=self.cluster.DATAPROC_PROJECT_ID,
            dataproc_cluster_name=self.cluster.DATAPROC_CLUSTER_NAME,
            dataproc_region=self.cluster.DATAPROC_REGION,
            dataproc_bucket=self.cluster.DATAPROC_BUCKET,
            destination_table_project_id=destination_table_project_id,
            data_quality_info=data_quality_info,
            destination_table_bucket=destination_table_bucket,
            destination_table_dataset=destination_table_dataset,
            destination_table=destination_table,
            destination_table_partition=cfg_table.PARTITION,
            destination_table_source_format=cfg_table.SOURCE_FORMAT,
            destination_table_flg_foto=cfg_table.FLG_FOTO,
            path_ingestion_control_partitions=path_ingestion_control_partitions,
            path_ingestion_control_count=path_ingestion_control_count,
        )

    # -------------------------------------------------------------
    def generate_script(
        self,
        dag: DAG,
        destination_table: str,
        cfg_table: IngestionKfk,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        create_table_if_needed: bool,
        environment_setup: Dict[str, Dict[str, Any]],
    ):
        return PythonOperator(
            dag=dag,
            task_id=f"trd_generate_script__{destination_table}",
            do_xcom_push=False,
            python_callable=self._generate_script,
            op_kwargs=dict(
                destination_table=destination_table,
                cfg_table=cfg_table,
                destination_table_project_id=destination_table_project_id,
                destination_table_bucket=destination_table_bucket,
                destination_table_dataset=destination_table_dataset,
                create_table_if_needed=create_table_if_needed,
                environment_setup=environment_setup,
            ),
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

    # -------------------------------------------------------------
    # TODO: Implements script generator for Jupyter Notebook
    def _generate_script(self, *args, **kwargs) -> None:
        ...
