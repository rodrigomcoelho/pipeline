from os.path import join
from pathlib import Path
from typing import Any, Dict, List

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from ..hooks.gcs_interface import GoogleDriveToGCSHookCustom
from ..hooks.gdrive_interface import GoogleDriveHookCustom
from ..templates import versioning
from ..types.cluster import SparkJob
from ..types.raw.cfg_ingestion_gdr import IngestionGdr
from .base_dag_patterns import BaseDagPatterns

# Temporary
from .old_dag_ingestion_file_patterns import DagIngestionFilePatterns as TmpOld


class DagIngestionFilePatterns(BaseDagPatterns):

    # Versão do template de criação das DAGs
    TEMPLATE_VERSION: float = versioning.RAW_INGESTION_GDR

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
        self.cfg_ingestion = self._adapter_result(
            self._load_ingestion_file(path_ingestion_config)
        )

    # -------------------------------------------------------------
    def _adapter_result(self, content_file: List[Dict[str, Any]]) -> List[IngestionGdr]:
        return [
            IngestionGdr(**table) for table in content_file if table["ACTIVE"] == "1"
        ]

    # -------------------------------------------------------------
    def _exec_search(self, ti, folder_id: str, file_name: str, idx: int) -> None:
        files = GoogleDriveHookCustom(
            gcp_conn_id=self.info_raw_ingestion["GCLOUD_CONN_ID"], logger=self.logging
        ).get_multiples_file_id(folder_id=folder_id, file_name=file_name)

        if len(files) < 1:
            raise ValueError("*** GDrive - File not found ***")

        ti.xcom_push(key=f"files_listed__{idx}", value=files)

    # -------------------------------------------------------------
    def _move_to_gcs(self, ti, file_ingestion: IngestionGdr, idx: int) -> None:
        files = ti.xcom_pull(key=f"files_listed__{idx}")

        gcs_interface = GoogleDriveToGCSHookCustom(
            gcp_conn_id=self.info_raw_ingestion["GCLOUD_CONN_ID"], logger=self.logging
        )

        for file in files:
            if file["mime_type"] in ["application/vnd.google-apps.spreadsheet"]:
                gcs_interface.move_google_spreadsheet_to_gcs(
                    spreadsheet_id=file["id"],
                    spreadsheet_name=file["name"],
                    spreadsheet_pages=file_ingestion.SPREADSHEET_PAGES
                    if type(file_ingestion.SPREADSHEET_PAGES) == list
                    else [file_ingestion.SPREADSHEET_PAGES],
                    bucket_name=self.info_raw_ingestion["ORIGINAL_BUCKET"],
                    object_name=join(
                        file_ingestion.FOLDER_PATH_FROM_INPUTBI,
                        Path(file["name"]).stem,
                        file_ingestion.RAW_TABLE,
                    ),
                    output_delimiter=file_ingestion.DELIMITER,
                    range_sheets=file_ingestion.RANGE_SHEETS,
                )
            else:
                gcs_interface.move_file_to_gcs(
                    file_id=file["id"],
                    file_name=file["name"],
                    bucket_name=self.info_raw_ingestion["ORIGINAL_BUCKET"],
                    object_name=join(
                        file_ingestion.FOLDER_PATH_FROM_INPUTBI,
                        Path(file["name"]).stem,
                        file_ingestion.RAW_TABLE,
                    ),
                )

    # -------------------------------------------------------------
    def define_spark_jobs(
        self, raw_spark_job: dict, trd_spark_job: dict, data_quality_spark_job: dict
    ):
        self.raw_spark_job = SparkJob(**raw_spark_job)
        self.trd_spark_job = SparkJob(**trd_spark_job)
        self.data_quality_spark_job = SparkJob(**data_quality_spark_job)

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
            db_type="INGESTION_FILE",
            region=region,
            tables=[file.RAW_TABLE for file in self.cfg_ingestion],
            suffix_cluster_name=self.dag_context,
            info_dataproc=self.info_dataproc,
            task_id_create=task_id_create,
            task_id_delete=task_id_delete,
        )

    # -------------------------------------------------------------
    def checking_files(
        self, dag: DAG, index: int, file: IngestionGdr
    ) -> PythonOperator:
        python_operator = PythonOperator(
            dag=dag,
            do_xcom_push=False,
            task_id=f"checking_files__{file.RAW_TABLE}",
            python_callable=self._exec_search,
            op_kwargs=dict(
                folder_id=file.FOLDER_ID, file_name=file.FILE_NAME, idx=index
            ),
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

        python_operator.pre_execute = lambda context: self._table_was_selected(
            tables=file.RAW_TABLE, context=context, launch_skip_exception=True
        )

        return python_operator

    # -------------------------------------------------------------
    def copy_gdrive_to_gcs(
        self, dag: DAG, index: int, file: IngestionGdr
    ) -> PythonOperator:
        return PythonOperator(
            dag=dag,
            task_id=f"copy_gdrive_to_gcs__{file.RAW_TABLE}",
            do_xcom_push=False,
            python_callable=self._move_to_gcs,
            op_kwargs=dict(file_ingestion=file, idx=index),
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

    # -------------------------------------------------------------
    def raw_update_table(
        self,
        dag: DAG,
        index: int,
        destination_table: str,
        file: IngestionGdr,
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
            task_id=f"raw_update_table__{destination_table}",
            spark_job=self.raw_spark_job,
            dataproc_project_id=self.cluster.DATAPROC_PROJECT_ID,
            dataproc_cluster_name=self.cluster.DATAPROC_CLUSTER_NAME,
            dataproc_region=self.cluster.DATAPROC_REGION,
            dataproc_bucket=self.cluster.DATAPROC_BUCKET,
            LAKE_PROJECT_ID=destination_table_project_id,
            ORIGIN_PATH=join(
                "gs://",
                self.info_raw_ingestion["ORIGINAL_BUCKET"],
                file.FOLDER_PATH_FROM_INPUTBI,
                Path(file.FILE_NAME).stem,
                file.RAW_TABLE,
            ),
            DESTINATION_BUCKET=destination_table_bucket,
            DATASET=destination_table_dataset,
            TABLE=file.RAW_TABLE,
            DELIMITER=file.DELIMITER,
            SPREADSHEET_PAGES=file.SPREADSHEET_PAGES
            if type(file.SPREADSHEET_PAGES) == list
            else [file.SPREADSHEET_PAGES],
            FILES="{{ task_instance.xcom_pull(key = '"
            + f"files_listed__{index}"
            + "') }}",
            PATH_INGESTION_CONTROL_PARTITIONS=path_ingestion_control_partitions,
            PATH_INGESTION_CONTROL_COUNT=path_ingestion_control_count,
            TRD_TABLE=file.TRD_TABLE,
            TRD_DATASET=destination_trd_table_dataset,
            CREATE_TABLE_IF_NEEDED=create_table_if_needed,
            RANGE_SHEETS=file.RANGE_SHEETS,
        )

    # -------------------------------------------------------------
    def trusted_update_table(
        self,
        dag: DAG,
        destination_table: str,
        file: IngestionGdr,
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
            destination_table=file.TRD_TABLE,
            destination_table_partition=None,
            destination_table_type_partition=None,
            destination_table_source_format=None,
            destination_table_mode=None,
            destination_table_flg_foto=None,
            origin_table_bucket=origin_table_bucket,
            origin_table_dataset=origin_table_dataset,
            origin_table=file.RAW_TABLE,
            create_table_if_needed=create_table_if_needed,
            format_sql_columns=file.FORMAT_SQL_COLUMNS,
            path_ingestion_control_partitions=path_ingestion_control_partitions,
            path_ingestion_control_count=path_ingestion_control_count,
        )

    # -------------------------------------------------------------
    def data_quality(
        self,
        dag: DAG,
        destination_table: str,
        file: IngestionGdr,
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
