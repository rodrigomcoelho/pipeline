# =========================================================================== #
#   This module is deprecated. Please use ./dag_ingestion_table_patterns.py   #
# =========================================================================== #

from collections import defaultdict

from airflow.utils.task_group import TaskGroup

from ..operators.custom_branch_python_operator import CustomBranchPythonOperator
from .old_base_dag_patterns import BaseDagPatterns


class DagIngestionTablePatterns(BaseDagPatterns):

    # -------------------------------------------------------------
    def __init__(
        self,
        dag_context: str,
        info_dataproc: dict,
        info_ingestion: dict,
        dag_connections: dict,
        dag_tables: list,
        log_layer: str,
        log_dag_name: str,
        log_task_name: str = None,
        log_identifier: str = None,
        log_logger: str = None,
        log_run_id: str = None,
        task_timeout: float = None,
    ):
        super().__init__(
            dag_context=dag_context,
            log_identifier=log_identifier,
            log_layer=log_layer,
            log_run_id=log_run_id,
            log_dag_name=log_dag_name,
            log_task_name=log_task_name,
            log_logger=log_logger,
            task_timeout=task_timeout,
        )

        self.info_dataproc = info_dataproc
        self.info_ingestion = info_ingestion
        self.dag_connections = dag_connections
        self.dag_tables = dag_tables

    # -------------------------------------------------------------
    def define_clusters(self, dag):
        self.clusters = dict()
        tables = defaultdict(lambda: dict(db_type=str(), region=str(), tables=set()))
        for table in self.dag_tables:
            db_type, region = (
                table["DB_TYPE"],
                self.dag_connections[table["DB_TYPE"]][table["DB_OWNER"]]["REGION"],
            )
            cluster_key = f"{db_type}_{region}".lower()
            tables[cluster_key]["db_type"] = db_type
            tables[cluster_key]["region"] = region
            tables[cluster_key]["tables"].add(table["TABLE"])

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
                info_ingestion=self.info_ingestion,
                task_id_create=f"create_dataproc_cluster_{key}",
                task_id_delete=f"delete_dataproc_cluster_{key}",
            )

        self.branching = CustomBranchPythonOperator(
            task_id="checking_required_clusters",
            dag=dag,
            python_callable=lambda clusters, **context: [
                cluster["CLUSTER_CREATED"].task_id
                for cluster in clusters
                if self._table_was_selected(set(cluster["TABLES"]), context)
            ],
            op_kwargs=dict(clusters=self.clusters.values()),
            execution_timeout=self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

    # -------------------------------------------------------------
    def update_tables(
        self,
        dag,
        task_group,
        spark_job,
        dataproc_project_id,
        dataproc_cluster_name,
        dataproc_region,
        dataproc_bucket,
        db_type,
        origin_table_url,
        origin_table_user,
        origin_table_password,
        origin_table_query,
        default_range_ingestion,
        destination_table_project_id,
        destination_table_bucket,
        destination_table_dataset,
        destination_table,
        ingestion_mode,
        bq_writedisposition,
        destination_table_partition,
        destination_table_type_partition,
        destination_table_clustered,
        destination_table_source_format,
        destination_table_expiration_ms,
        destination_table_flg_foto,
    ):
        return self._ingestion(
            dag=dag,
            task_group=task_group,
            task_id=f"update_table_{destination_table}",
            spark_job=spark_job,
            dataproc_project_id=dataproc_project_id,
            dataproc_cluster_name=dataproc_cluster_name,
            dataproc_region=dataproc_region,
            dataproc_bucket=dataproc_bucket,
            table_to_check_payload=destination_table,
            DB_TYPE=db_type,
            URL=origin_table_url,
            USER=origin_table_user,
            PASSWORD=origin_table_password,
            QUERY=origin_table_query,
            ENV_RANGE_INGESTION=default_range_ingestion,
            DATE_SETUP=dict(
                RANGE_INGESTION="{{ dag_run.conf.get('RANGE_INGESTION', '') }}",
                START_DATE="{{ dag_run.conf.get('START_DATE', '') }}",
                END_DATE="{{ dag_run.conf.get('END_DATE', '') }}",
            ),
            PROJECT_ID=destination_table_project_id,
            BUCKET=destination_table_bucket,
            DATASET=destination_table_dataset,
            TABLE=destination_table,
            MODE=ingestion_mode,
            BQ_WRITEDISPOSITION=bq_writedisposition,
            PARTITION=destination_table_partition,
            TYPE_PARTITION=destination_table_type_partition,
            CLUSTERED=destination_table_clustered,
            SOURCE_FORMAT=destination_table_source_format,
            EXPIRATION_MS=destination_table_expiration_ms,
            FLG_FOTO=destination_table_flg_foto,
        )

    # -------------------------------------------------------------
    def data_quality(
        self,
        dag,
        task_group,
        spark_job,
        dataproc_project_id,
        dataproc_cluster_name,
        dataproc_region,
        dataproc_bucket,
        data_quality_info,
        destination_table_project_id,
        destination_table_bucket,
        destination_table_dataset,
        destination_table,
        destination_table_partition,
        destination_table_source_format,
        destination_table_flg_foto,
    ):
        return self._data_quality(
            dag=dag,
            task_group=task_group,
            task_id=f"data_quality_{destination_table}",
            spark_job=spark_job,
            dataproc_project_id=dataproc_project_id,
            dataproc_cluster_name=dataproc_cluster_name,
            dataproc_region=dataproc_region,
            dataproc_bucket=dataproc_bucket,
            destination_table_project_id=destination_table_project_id,
            data_quality_info=data_quality_info,
            destination_table_bucket=destination_table_bucket,
            destination_table_dataset=destination_table_dataset,
            destination_table=destination_table,
            destination_table_partition=destination_table_partition,
            destination_table_source_format=destination_table_source_format,
            destination_table_flg_foto=destination_table_flg_foto,
        )
