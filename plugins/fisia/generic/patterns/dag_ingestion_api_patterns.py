from typing import Any, Dict, List

from airflow.models import DAG

from ..templates import versioning
from ..types.cluster import SparkJob
from ..types.raw.cfg_ingestion_api import IngestionAPI
from .base_dag_patterns import BaseDagPatterns


class DagIngestionAPIPatterns(BaseDagPatterns):

    # Versão do template de criação das DAGs
    TEMPLATE_VERSION: float = versioning.RAW_INGESTION_API

    # -------------------------------------------------------------
    def __init__(
        self,
        script_version: float,
        layer: str,
        domain: str,
        dag_name: str,
        dag_context: str,
        info_kube_pod: dict,
        info_dataproc: dict,
        path_ingestion_config: str,
        info_raw_ingestion: dict = None,
        info_trd_ingestion: dict = None,
        info_quality_ingestion: dict = None,
        log_task_name: str = None,
        log_identifier: str = None,
        log_logger: str = None,
        log_run_id: str = None,
        log_repository: str = None,
        task_timeout: float = None,
    ):
        super().__init__(
            script_version=script_version,
            dag_context=dag_context,
            log_identifier=log_identifier,
            layer=layer,
            domain=domain,
            log_run_id=log_run_id,
            dag_name=dag_name,
            log_task_name=log_task_name,
            log_logger=log_logger,
            task_timeout=task_timeout,
            log_repository=log_repository,
        )

        self.info_kube_pod = info_kube_pod
        self.info_dataproc = info_dataproc
        self.info_raw_ingestion = info_raw_ingestion
        self.info_trd_ingestion = info_trd_ingestion
        self.info_quality_ingestion = info_quality_ingestion
        self.cfg_ingestion = self._adapter_result(
            self._load_ingestion_file(path_ingestion_config)
        )

    # -------------------------------------------------------------
    def _adapter_result(self, content_file: List[Dict[str, Any]]) -> List[IngestionAPI]:
        return [
            IngestionAPI(**table) for table in content_file if table["ACTIVE"] == "1"
        ]

    # -------------------------------------------------------------
    def define_spark_jobs(self, trd_spark_job: dict, data_quality_spark_job: dict):
        self.trd_spark_job = SparkJob(**trd_spark_job)
        self.data_quality_spark_job = SparkJob(**data_quality_spark_job)

    # -------------------------------------------------------------
    def define_date_setup(self, dag: DAG, default_range_ingestion: int):
        return self._define_date_setup(
            dag=dag,
            default_range_ingestion=default_range_ingestion,
            data_lake_tables_name=[table.RAW_TABLE for table in self.cfg_ingestion],
        )

    # -------------------------------------------------------------
    def define_clusters(
        self,
        dag: DAG,
        region: str = "US",
        task_id_create: str = "create_dataproc_cluster",
        task_id_delete: str = "delete_dataproc_cluster",
    ):
        self.cluster = self._define_cluster(
            dag=dag,
            db_type="INGESTION_API",
            region=region,
            tables=[file.RAW_TABLE for file in self.cfg_ingestion],
            suffix_cluster_name=self.dag_context,
            info_dataproc=self.info_dataproc,
            task_id_create=task_id_create,
            task_id_delete=task_id_delete,
        )

    # -------------------------------------------------------------
    def raw_update_table(
        self,
        dag: DAG,
        destination_table: str,
        cfg_table: IngestionAPI,
        platform_project_id: str,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        path_ingestion_control_partitions: str,
        path_ingestion_control_count: str,
        destination_trd_table_dataset: str,
        create_table_if_needed: bool,
        custom_consumer_path: str = None,
    ):
        pre_execute = lambda context: self._table_was_selected(
            tables=destination_table, context=context, launch_skip_exception=True
        )

        custom_consumer_path = custom_consumer_path or self.info_raw_ingestion[
            "CUSTOM_CONSUMER_PATH"
        ].format(
            REPOSITORY=self.log_repository.lower(),
            LAYER=self.layer.lower(),
            DAG_NAME=self.dag_name.lower(),
        )

        return self._launch_pod_operator(
            dag=dag,
            task_id=f"raw_update_table__{destination_table}",
            gke_project_id=self.info_kube_pod["GKE_PROJECT_ID"],
            gke_cluster_name=self.info_kube_pod["GKE_CLUSTER_NAME"],
            gke_region=self.info_kube_pod["GKE_REGION"],
            gke_namespace=self.info_kube_pod["GKE_NAMESPACE"],
            gke_service_account_name=self.info_kube_pod["GKE_SERVICE_ACCOUNT_NAME"],
            gke_affinity=self.info_kube_pod["AFFINITY"],
            gke_tolerations=self.info_kube_pod["TOLERATIONS"],
            gcp_conn_id=self.info_kube_pod["GCP_CONN_ID"],
            image=self.info_raw_ingestion["IMAGE"],
            labels=self.info_kube_pod["GKE_LABELS"],
            pre_execute=pre_execute,
            START_DATE="{{ ti.xcom_pull(key = '"
            + f"SETUP_{destination_table}".upper()
            + "')['START_DATE'] }}",
            END_DATE="{{ ti.xcom_pull(key = '"
            + f"SETUP_{destination_table}".upper()
            + "')['END_DATE'] }}",
            PLATFORM_PROJECT_ID=platform_project_id,
            PROJECT_ID=destination_table_project_id,
            BUCKET=destination_table_bucket,
            DATASET=destination_table_dataset,
            TABLE=destination_table,
            CUSTOM_CONSUMER_BUCKET=self.info_raw_ingestion["CUSTOM_CONSUMER_BUCKET"],
            CUSTOM_CONSUMER_PATH=custom_consumer_path,
            CONNECTIONS_API=self.info_raw_ingestion["CONNECTIONS_API"],
            ENDPOINT_CONFIG=cfg_table.ENDPOINT_CONFIG,
            PARTITION=cfg_table.PARTITION,
            FLG_FOTO=cfg_table.FLG_FOTO,
            FORMAT_OUTPUT=cfg_table.FORMAT_OUTPUT,
            RESPONSE_BUCKET=self.info_raw_ingestion["RESPONSE_BUCKET"],
            TRANSIENT_TYPE_PARTITION=cfg_table.TRANSIENT_TYPE_PARTITION,
            PATH_INGESTION_CONTROL_PARTITIONS=path_ingestion_control_partitions,
            PATH_INGESTION_CONTROL_COUNT=path_ingestion_control_count,
            TRD_TABLE=cfg_table.TRD_TABLE,
            TRD_DATASET=destination_trd_table_dataset,
            CREATE_TABLE_IF_NEEDED=create_table_if_needed,
        )

    # -------------------------------------------------------------
    def trusted_update_table(
        self,
        dag: DAG,
        destination_table: str,
        cfg_table: IngestionAPI,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        origin_table_bucket: str,
        origin_table_dataset: str,
        create_table_if_needed: bool,
        path_ingestion_control_partitions: str,
        path_ingestion_control_count: str,
    ):
        return self._ingestion_trusted(
            dag=dag,
            task_id=f"trd_update_table__{destination_table}",
            cluster=self.cluster,
            spark_job=self.trd_spark_job,
            destination_table_project_id=destination_table_project_id,
            destination_table_bucket=destination_table_bucket,
            destination_table_dataset=destination_table_dataset,
            destination_table=cfg_table.TRD_TABLE,
            destination_table_partition=cfg_table.PARTITION,
            destination_table_type_partition=None,
            destination_table_source_format=None,
            destination_table_mode=None,
            destination_table_flg_foto=cfg_table.FLG_FOTO,
            origin_table_bucket=origin_table_bucket,
            origin_table_dataset=origin_table_dataset,
            origin_table=cfg_table.RAW_TABLE,
            create_table_if_needed=create_table_if_needed,
            format_sql_columns=cfg_table.FORMAT_SQL_COLUMNS,
            path_ingestion_control_partitions=path_ingestion_control_partitions,
            path_ingestion_control_count=path_ingestion_control_count,
        )

    # -------------------------------------------------------------
    def data_quality(
        self,
        dag: DAG,
        destination_table: str,
        cfg_table: IngestionAPI,
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
            destination_table_partition=None,
            destination_table_source_format=None,
            destination_table_flg_foto=None,
            path_ingestion_control_partitions=path_ingestion_control_partitions,
            path_ingestion_control_count=path_ingestion_control_count,
        )
