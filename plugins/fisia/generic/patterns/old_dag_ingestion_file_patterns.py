# ========================================================================= #
#   This module is deprecated. Please use ./dag_ingestion_gdr_patterns.py   #
# ========================================================================= #

from os.path import join
from pathlib import Path

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from ..hooks.gcs_interface import GoogleDriveToGCSHookCustom
from ..hooks.gdrive_interface import GoogleDriveHookCustom
from .old_base_dag_patterns import BaseDagPatterns


class DagIngestionFilePatterns(BaseDagPatterns):

    # -------------------------------------------------------------
    def __init__(
        self,
        dag_context: str,
        info_dataproc: dict,
        info_ingestion: dict,
        dag_files: list,
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
        self.dag_files = dag_files

    # -------------------------------------------------------------
    def _exec_search(self, ti, folder_id: str, file_name: str, idx: int) -> None:
        files = GoogleDriveHookCustom(
            gcp_conn_id=self.info_ingestion["GCLOUD_CONN_ID"], logger=self.logging
        ).get_multiples_file_id(folder_id=folder_id, file_name=file_name)

        if len(files) < 1:
            raise ValueError("*** GDrive - File not found ***")

        ti.xcom_push(key=f"files_listed__{idx}", value=files)

    # -------------------------------------------------------------
    def _move_to_gcs(self, ti, file_ingestion: dict, idx: int) -> None:
        files = ti.xcom_pull(key=f"files_listed__{idx}")

        gcs_interface = GoogleDriveToGCSHookCustom(
            gcp_conn_id=self.info_ingestion["GCLOUD_CONN_ID"], logger=self.logging
        )

        for file in files:
            if file["mime_type"] in ["application/vnd.google-apps.spreadsheet"]:
                gcs_interface.move_google_spreadsheet_to_gcs(
                    spreadsheet_id=file["id"],
                    spreadsheet_name=file["name"],
                    spreadsheet_pages=file_ingestion["SPREADSHEET_PAGES"]
                    if type(file_ingestion["SPREADSHEET_PAGES"]) == list
                    else [file_ingestion["SPREADSHEET_PAGES"]],
                    bucket_name=self.info_ingestion["ORIGINAL_BUCKET"],
                    object_name=join(
                        file_ingestion["FOLDER_PATH_FROM_INPUTBI"],
                        Path(file["name"]).stem,
                        file_ingestion["TABLE"],
                    ),
                    output_delimiter=file_ingestion["DELIMITER"],
                )
            else:
                gcs_interface.move_file_to_gcs(
                    file_id=file["id"],
                    file_name=file["name"],
                    bucket_name=self.info_ingestion["ORIGINAL_BUCKET"],
                    object_name=join(
                        file_ingestion["FOLDER_PATH_FROM_INPUTBI"],
                        Path(file["name"]).stem,
                        file_ingestion["TABLE"],
                    ),
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
            db_type="INGESTION_FILE",
            region=region,
            tables=[file["TABLE"] for file in self.dag_files],
            suffix_cluster_name=self.dag_context,
            info_dataproc=self.info_dataproc,
            info_ingestion=self.info_ingestion,
            task_id_create=task_id_create,
            task_id_delete=task_id_delete,
        )

    # -------------------------------------------------------------
    def checking_files(self, dag: DAG, index: int, file: dict) -> PythonOperator:
        python_operator = PythonOperator(
            dag=dag,
            do_xcom_push=False,
            task_id=f"checking_files__{file['TABLE']}",
            python_callable=self._exec_search,
            op_kwargs=dict(
                folder_id=file["FOLDER_ID"], file_name=file["FILE_NAME"], idx=index
            ),
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

        python_operator.pre_execute = lambda context: self._table_was_selected(
            tables=file["TABLE"], context=context, launch_skip_exception=True
        )

        return python_operator

    # -------------------------------------------------------------
    def copy_gdrive_to_gcs(self, dag: DAG, index: int, file: dict) -> PythonOperator:
        return PythonOperator(
            dag=dag,
            task_id=f"copy_gdrive_to_gcs__{file['TABLE']}",
            do_xcom_push=False,
            python_callable=self._move_to_gcs,
            op_kwargs=dict(file_ingestion=file, idx=index),
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

    # -------------------------------------------------------------
    def ingestion_file_task(
        self,
        dag: DAG,
        index: int,
        file: dict,
        destination_table_bucket: str,
        destination_table_dataset: str,
        destination_table_project_id: str,
    ):
        return self._ingestion(
            dag=dag,
            task_id=f"ingestion_file__{file['TABLE']}",
            spark_job=self.cluster["SPARK_JOBS"]["INGESTION"],
            dataproc_project_id=self.cluster["DATAPROC_PROJECT_ID"],
            dataproc_cluster_name=self.cluster["DATAPROC_CLUSTER_NAME"],
            dataproc_region=self.cluster["DATAPROC_REGION"],
            dataproc_bucket=self.cluster["DATAPROC_BUCKET"],
            LAKE_PROJECT_ID=destination_table_project_id,
            ORIGIN_PATH=join(
                "gs://",
                self.info_ingestion["ORIGINAL_BUCKET"],
                file["FOLDER_PATH_FROM_INPUTBI"],
                Path(file["FILE_NAME"]).stem,
                file["TABLE"],
            ),
            DESTINATION_BUCKET=destination_table_bucket,
            DATASET=destination_table_dataset,
            TABLE=file["TABLE"],
            DELIMITER=file["DELIMITER"],
            SPREADSHEET_PAGES=file["SPREADSHEET_PAGES"]
            if type(file["SPREADSHEET_PAGES"]) == list
            else [file["SPREADSHEET_PAGES"]],
            FILES="{{ task_instance.xcom_pull(key = '"
            + f"files_listed__{index}"
            + "') }}",
        )

    # -------------------------------------------------------------
    def data_quality(
        self,
        dag: DAG,
        data_quality_info: dict,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        destination_table: str,
    ):
        return self._data_quality(
            dag=dag,
            task_id=f"data_quality_{destination_table}",
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
            destination_table_partition=None,
            destination_table_source_format=None,
            destination_table_flg_foto=None,
        )
