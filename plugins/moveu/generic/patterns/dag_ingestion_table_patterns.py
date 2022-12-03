from collections import defaultdict
from typing import Any, Dict, List

from airflow.models import DAG
from airflow.utils.task_group import TaskGroup

from ..operators.custom_branch_python_operator import CustomBranchPythonOperator
from ..templates import versioning
from ..types.cluster import Cluster, SparkJob
from ..types.raw.cfg_ingestion_tables import IngestionTables
from .base_dag_patterns import BaseDagPatterns

# Temporary
from .old_dag_ingestion_table_patterns import DagIngestionTablePatterns as TmpOld


class DagIngestionTablePatterns(BaseDagPatterns):

    # Versão do template de criação das DAGs
    TEMPLATE_VERSION: float = versioning.RAW_INGESTION_TABLE

    # Temporary
    def __new__(cls, *args, **kwargs):
        if "script_version" not in kwargs:
            obj = object.__new__(TmpOld)
            obj.__init__(*args, **kwargs)
            return obj

        return super().__new__(cls, *args, **kwargs)

    # -------------------------------------------------------------
    def __init__(
        self,
        script_version: float,
        layer: str,
        domain: str,
        dag_name: str,
        dag_context: str,
        info_dataproc: dict,
        dag_connections: dict,
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

        self.info_dataproc = info_dataproc
        self.info_raw_ingestion = info_raw_ingestion
        self.info_trd_ingestion = info_trd_ingestion
        self.info_quality_ingestion = info_quality_ingestion
        self.dag_connections = dag_connections
        self.cfg_ingestion = self._adapter_result(
            self._load_ingestion_file(path_ingestion_config)
        )

    # -------------------------------------------------------------
    def _adapter_result(
        self, content_file: List[Dict[str, Any]]
    ) -> List[IngestionTables]:
        return [
            IngestionTables(**table) for table in content_file if table["ACTIVE"] == "1"
        ]

    # -------------------------------------------------------------
    def define_spark_jobs(
        self, raw_spark_job: dict, trd_spark_job: dict, data_quality_spark_job: dict
    ):
        self.raw_spark_job = SparkJob(**raw_spark_job)
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
    def define_clusters(self, dag):
        self.clusters: Dict[str, Cluster] = dict()
        tables = defaultdict(lambda: dict(db_type=str(), region=str(), tables=set()))
        for table in self.cfg_ingestion:
            db_type, region = (
                table.DB_TYPE,
                self.dag_connections[table.DB_TYPE][table.DB_OWNER]["REGION"],
            )
            cluster_key = f"{db_type}_{region}".lower()
            tables[cluster_key]["db_type"] = db_type
            tables[cluster_key]["region"] = region
            tables[cluster_key]["tables"].add(table.RAW_TABLE)

        for key, values in tables.items():
            cluster_group = TaskGroup(
                group_id=key.upper(),
                prefix_group_id=True,
                ui_color="#E6E6E6",
                ui_fgcolor="#000",
                dag=dag,
            )

            self.clusters[key] = self._define_cluster(
                dag=dag,
                task_group=cluster_group,
                db_type=values["db_type"],
                region=values["region"],
                tables=list(values["tables"]),
                suffix_cluster_name=f"{values['db_type']}-{values['region']}-{self.dag_context}",
                info_dataproc=self.info_dataproc,
                task_id_create=f"create_dataproc_cluster_{key}",
                task_id_delete=f"delete_dataproc_cluster_{key}",
            )

        self.branching = CustomBranchPythonOperator(
            task_id="checking_required_clusters",
            dag=dag,
            python_callable=lambda clusters, **context: [
                cluster.CLUSTER_CREATED.task_id
                for cluster in clusters
                if self._table_was_selected(set(cluster.TABLES), context)
            ],
            op_kwargs=dict(clusters=self.clusters.values()),
            execution_timeout=self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

    # -------------------------------------------------------------
    def get_cluster_for_table(self, table_name: str) -> Cluster:
        for cluster in self.clusters.values():
            if table_name in cluster.TABLES:
                return cluster

        raise ValueError(
            f"Table `{table_name}` has not been associated with any cluster"
        )

    # -------------------------------------------------------------
    def raw_update_table(
        self,
        dag: DAG,
        task_group: TaskGroup,
        cluster: Cluster,
        destination_table: str,
        cfg_table: IngestionTables,
        secret_id_jdbc_credentials: str,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        path_ingestion_control_partitions: str,
        path_ingestion_control_count: str,
        destination_trd_table_dataset: str,
        create_table_if_needed: bool,
    ):
        return self._ingestion(
            dag=dag,
            task_group=task_group,
            task_id=f"raw_update_table__{destination_table}",
            spark_job=self.raw_spark_job,
            dataproc_project_id=cluster.DATAPROC_PROJECT_ID,
            dataproc_cluster_name=cluster.DATAPROC_CLUSTER_NAME,
            dataproc_region=cluster.DATAPROC_REGION,
            dataproc_bucket=cluster.DATAPROC_BUCKET,
            table_name=destination_table,
            DB_TYPE=cfg_table.DB_TYPE,
            DB_OWNER=cfg_table.DB_OWNER,
            QUERY=cfg_table.QUERY,
            SECRET_ID_JDBC_CREDENTIALS=secret_id_jdbc_credentials,
            START_DATE="{{ ti.xcom_pull(key = '"
            + f"SETUP_{destination_table}".upper()
            + "')['START_DATE'] }}",
            END_DATE="{{ ti.xcom_pull(key = '"
            + f"SETUP_{destination_table}".upper()
            + "')['END_DATE'] }}",
            PROJECT_ID=destination_table_project_id,
            BUCKET=destination_table_bucket,
            DATASET=destination_table_dataset,
            TABLE=destination_table,
            MODE=cfg_table.MODE,
            BQ_WRITEDISPOSITION=cfg_table.BQ_WRITEDISPOSITION,
            PARTITION=cfg_table.PARTITION,
            TYPE_PARTITION=cfg_table.TYPE_PARTITION,
            CLUSTERED=cfg_table.CLUSTERED,
            SOURCE_FORMAT=cfg_table.SOURCE_FORMAT,
            EXPIRATION_MS=cfg_table.EXPIRATION_MS,
            FLG_FOTO=cfg_table.FLG_FOTO,
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
        task_group: TaskGroup,
        cluster: Cluster,
        destination_table: str,
        cfg_table: IngestionTables,
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
            task_group=task_group,
            cluster=cluster,
            spark_job=self.trd_spark_job,
            destination_table_project_id=destination_table_project_id,
            destination_table_bucket=destination_table_bucket,
            destination_table_dataset=destination_table_dataset,
            destination_table=cfg_table.TRD_TABLE,
            destination_table_partition=cfg_table.PARTITION,
            destination_table_type_partition=cfg_table.TYPE_PARTITION,
            destination_table_source_format=cfg_table.SOURCE_FORMAT,
            destination_table_mode=cfg_table.MODE,
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
        task_group: TaskGroup,
        cluster: Cluster,
        destination_table: str,
        cfg_table: IngestionTables,
        data_quality_info: dict,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        path_ingestion_control_partitions: str,
        path_ingestion_control_count: str,
    ):
        return self._data_quality(
            dag=dag,
            task_group=task_group,
            task_id=f"data_quality__{destination_table}",
            spark_job=self.data_quality_spark_job,
            dataproc_project_id=cluster.DATAPROC_PROJECT_ID,
            dataproc_cluster_name=cluster.DATAPROC_CLUSTER_NAME,
            dataproc_region=cluster.DATAPROC_REGION,
            dataproc_bucket=cluster.DATAPROC_BUCKET,
            data_quality_info=data_quality_info,
            destination_table_project_id=destination_table_project_id,
            destination_table_bucket=destination_table_bucket,
            destination_table_dataset=destination_table_dataset,
            destination_table=destination_table,
            destination_table_partition=cfg_table.PARTITION,
            destination_table_source_format=cfg_table.SOURCE_FORMAT,
            destination_table_flg_foto=cfg_table.FLG_FOTO,
            path_ingestion_control_partitions=path_ingestion_control_partitions,
            path_ingestion_control_count=path_ingestion_control_count,
        )
