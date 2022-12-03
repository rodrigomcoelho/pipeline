import json

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from .old_base_dag_patterns import BaseDagPatterns


class DagIngestionAWSHistoric(BaseDagPatterns):

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
            db_type="INGESTION_GENERIC",
            region=region,
            suffix_cluster_name="awshist-{{ dag_run.conf['ID']}}".lower(),
            info_dataproc=self.info_dataproc,
            info_ingestion=self.info_ingestion,
            task_id_create="create_dataproc_cluster",
            task_id_delete="delete_dataproc_cluster",
        )

    # -------------------------------------------------------------
    def _check_payload_conf(self, config: str):
        config = json.loads(config)
        self.logging.info(f">>> PAYLOAD =>> {config}")
        if not ("LOAD_TYPE" in config and ("RAW" in config or "REFINED" in config)):
            self.logging.error("<<< Invalid PAYLOAD >>> ")
            raise AirflowException

    # -------------------------------------------------------------
    def check_payload(self, dag: DAG) -> PythonOperator:
        return PythonOperator(
            task_id="check_payload",
            python_callable=self._check_payload_conf,
            op_kwargs=dict(config="{{ dag_run.conf | tojson}}"),
            execution_timeout=self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
            dag=dag,
        )

    # -------------------------------------------------------------
    def ingestion_aws_historic(self, dag: DAG, project_lake: str):

        return self._submit_job(
            dag=dag,
            task_id=f"ingestion_aws_historic",
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
            PROJECT_LAKE=project_lake,
            PAYLOAD_DAG="{{ dag_run.conf | forceescape }}",
            CREATE_TABLE=self.info_ingestion["CREATE_TABLE"],
            RAW_PATH=self.info_ingestion["RAW"]["PATH"],
            RAW_DATASET=self.info_ingestion["RAW"]["DATASET"],
            TRUSTED_PATH=self.info_ingestion["TRUSTED"]["PATH"],
            TRUSTED_DATASET=self.info_ingestion["TRUSTED"]["DATASET"],
            REFINED_PATH=self.info_ingestion["REFINED"]["PATH"],
            REFINED_DATASET=self.info_ingestion["REFINED"]["DATASET"],
        )
