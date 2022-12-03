# ================================================================ #
#   This module is deprecated. Please use ./base_dag_patterns.py   #
# ================================================================ #

import json
import os
from datetime import date, timedelta
from typing import Callable, Dict, List, Union

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
)
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from ..utils.custom_print import default_logging
from .logging_patterns import LoggingPatterns


class BaseDagPatterns:

    # -------------------------------------------------------------
    def __init__(
        self,
        dag_context: str,
        log_layer: str,
        log_dag_name: str,
        log_task_name: str = None,
        log_logger: str = None,
        log_identifier: str = None,
        log_run_id: str = None,
        task_timeout: float = None,
    ) -> None:
        self.dag_context = dag_context
        self.task_timeout = timedelta(seconds=task_timeout) if task_timeout else None

        self.logging = LoggingPatterns(
            logger=log_logger,
            identifier=log_identifier,
            layer=log_layer,
            airflow_run_id=log_run_id,
            airflow_context=dag_context,
            airflow_dag_name=log_dag_name,
            airflow_task_name=log_task_name,
        )

    # -------------------------------------------------------------
    def _validate_payload(self, payload: dict):
        default_logging("Submitted payload", "-", json.dumps(payload, indent=4))

        if payload.get("START_DATE") or payload.get("END_DATE"):
            assert date.fromisoformat(payload["END_DATE"]) >= date.fromisoformat(
                payload["START_DATE"]
            ), "`END_DATE` parameter must be greater than `START_DATE`"

        if payload.get("RANGE_INGESTION"):
            assert str(
                payload["RANGE_INGESTION"]
            ).isnumeric(), "`RANGE_INGESTION` parameter must be numeric"

        if payload.get("TABLES"):
            assert isinstance(
                payload["TABLES"], (str, list)
            ), "`TABLES` parameter must be of type `str` or `list`"

    # -------------------------------------------------------------
    def created_start_job(
        self, dag: DAG, task_id: str = "start_job", task_timeout: timedelta = None
    ) -> PythonOperator:
        return PythonOperator(
            task_id=task_id,
            python_callable=lambda ti, dag_run, dag_context, now: (
                self._validate_payload(dag_run.conf),
                ti.xcom_push(key="run_id", value=f"{dag_context}_{now}"),
            ),
            op_kwargs={"dag_context": self.dag_context, "now": "{{ ts_nodash }}"},
            execution_timeout=task_timeout or self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
            dag=dag,
        )

    # -------------------------------------------------------------
    def complete_process(
        self,
        dag: DAG,
        task_id: str = "complete_process",
        task_timeout: timedelta = None,
    ) -> DummyOperator:
        return DummyOperator(
            task_id=task_id,
            trigger_rule=TriggerRule.ALL_DONE,
            execution_timeout=task_timeout or self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
            dag=dag,
        )

    # -------------------------------------------------------------
    def _define_cluster(
        self,
        dag: DAG,
        db_type: str,
        region: str,
        suffix_cluster_name: str,
        info_dataproc: dict,
        info_ingestion: dict,
        task_id_create: str,
        task_id_delete: str,
        task_timeout: timedelta = None,
        task_group: TaskGroup = None,
        tables: list = None,
        **kwargs,
    ) -> dict:
        cluster_vars = {
            "DB_TYPE": db_type,
            "REGION": region,
            "TABLES": tables,
            "DATAPROC_CLUSTER_NAME": info_dataproc["DATAPROC_CLUSTER_NAME"]
            .format(suffix_cluster_name)
            .lower(),
            "DATAPROC_PROJECT_ID": info_dataproc["PROJECT_ID"],
            "DATAPROC_BUCKET": info_dataproc[f"INFO_{region}"]["DATAPROC_BUCKET"],
            "DATAPROC_REGION": info_dataproc[f"INFO_{region}"]["DATAPROC_REGION"],
            "DATAPROC_ZONE": info_dataproc[f"INFO_{region}"]["DATAPROC_ZONE"],
            "DATAPROC_SERVICE_ACCOUNT": info_dataproc["DATAPROC_SERVICE_ACCOUNT"],
            "DATAPROC_M_MACHINE_TYPE": info_dataproc["DATAPROC_M_MACHINE_TYPE"][
                db_type
            ],
            "DATAPROC_W_MACHINE_TYPE": info_dataproc["DATAPROC_W_MACHINE_TYPE"],
            "DATAPROC_SUBNETWORK_URI": info_dataproc[f"INFO_{region}"][
                "DATAPROC_SUBNETWORK_URI"
            ],
            "DATAPROC_INIT_ACTIONS_URIS": info_dataproc["DATAPROC_INIT_ACTIONS_URIS"],
            "TAGS": info_dataproc["TAGS"],
            "DATAPROC_AUTOSCALING_POLICY": info_dataproc[f"INFO_{region}"][
                "AUTOSCALING_POLICY"
            ],
            "FLG_DELETE_CLUSTER": int(info_dataproc["FLG_DELETE_CLUSTER"]),
            "SPARK_JOBS": {
                "INGESTION": info_ingestion["INGESTION_SPARK_JOB_PATH"],
                "DATA_QUALITY": info_ingestion["DATA_QUALITY_SPARK_JOB_PATH"],
            },
            **kwargs,
            "CLUSTER_GROUP": task_group,
            "CLUSTER_CREATED": "",
            "CLUSTER_DELETE": "",
        }

        cluster_vars["CLUSTER_CREATED"] = DataprocClusterCreateOperator(
            task_id=task_id_create,
            cluster_name=cluster_vars["DATAPROC_CLUSTER_NAME"],
            project_id=cluster_vars["DATAPROC_PROJECT_ID"],
            num_workers=2,
            zone=cluster_vars["DATAPROC_ZONE"],
            region=cluster_vars["DATAPROC_REGION"],
            storage_bucket=cluster_vars["DATAPROC_BUCKET"],
            subnetwork_uri=cluster_vars["DATAPROC_SUBNETWORK_URI"],
            internal_ip_only=True,
            init_actions_uris=cluster_vars["DATAPROC_INIT_ACTIONS_URIS"],
            properties={
                "dataproc:pip.packages": "google-cloud-bigquery==2.20.0,fs-gcsfs==1.4.1,requests==2.25.1,cx_oracle==8.2.1,slack-sdk==3.15.2,great-expectations==0.13.30,google-cloud-secret-manager==2.10.0",
                "dataproc:dataproc.conscrypt.provider.enable": "false",
                "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
                "dataproc:dataproc.logging.stackdriver.enable": "true",
                "dataproc:jobs.file-backed-output.enable": "true",
            },
            autoscaling_policy=cluster_vars["DATAPROC_AUTOSCALING_POLICY"],
            use_if_exists=True,
            tags=cluster_vars["TAGS"],
            master_machine_type=cluster_vars["DATAPROC_M_MACHINE_TYPE"],
            worker_machine_type=cluster_vars["DATAPROC_W_MACHINE_TYPE"],
            service_account=cluster_vars["DATAPROC_SERVICE_ACCOUNT"],
            execution_timeout=task_timeout or self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
            dag=dag,
            task_group=task_group,
        )

        cluster_vars["CLUSTER_DELETE"] = (
            DataprocClusterDeleteOperator(
                task_id=task_id_delete,
                project_id=cluster_vars["DATAPROC_PROJECT_ID"],
                cluster_name=cluster_vars["DATAPROC_CLUSTER_NAME"],
                region=cluster_vars["DATAPROC_REGION"],
                trigger_rule=TriggerRule.ALL_DONE,
                execution_timeout=task_timeout or self.task_timeout,
                on_execute_callback=self.logging.log_task_start,
                on_failure_callback=self.logging.log_task_error,
                on_success_callback=self.logging.log_task_success,
                on_retry_callback=self.logging.log_task_retry,
                dag=dag,
                task_group=task_group,
            )
            if cluster_vars["FLG_DELETE_CLUSTER"]
            else DummyOperator(
                task_id=task_id_delete,
                trigger_rule=TriggerRule.ALL_DONE,
                execution_timeout=task_timeout or self.task_timeout,
                on_execute_callback=self.logging.log_task_start,
                on_failure_callback=self.logging.log_task_error,
                on_success_callback=self.logging.log_task_success,
                on_retry_callback=self.logging.log_task_retry,
                dag=dag,
                task_group=task_group,
            )
        )

        return cluster_vars

    # -------------------------------------------------------------
    def _table_was_selected(
        self, tables: Union[str, set], context, launch_skip_exception: bool = False
    ) -> bool:
        tables_config = context["dag_run"].conf.get("TABLES")
        if not tables_config or tables_config in ("*", [""], ["*"]):
            return True

        if not isinstance(tables_config, list):
            raise ValueError(
                f"Typing of `TABLES` is invalid, expect `list` not `{type(tables_config)}`"
            )

        if not isinstance(tables, (str, set)):
            raise ValueError(
                f"Typing of `tables` is invalid, expect `str` or `set` not `{type(tables)}`"
            )

        tables = tables if isinstance(tables, set) else set([tables])
        if tables & set(tables_config):
            return True

        if launch_skip_exception:
            raise AirflowSkipException

        return False

    # -------------------------------------------------------------
    def _ingestion(
        self,
        dag: DAG,
        task_id: str,
        spark_job: Dict[str, str],
        dataproc_project_id: str,
        dataproc_cluster_name: str,
        dataproc_region: str,
        dataproc_bucket: str,
        task_timeout: int = None,
        task_group: TaskGroup = None,
        table_to_check_payload: str = None,
        **args,
    ) -> DataprocSubmitJobOperator:
        pre_execute = lambda context: self._table_was_selected(
            tables=table_to_check_payload, context=context, launch_skip_exception=True
        )

        return self._submit_job(
            dag=dag,
            task_group=task_group,
            task_id=task_id,
            dataproc_project_id=dataproc_project_id,
            dataproc_cluster_name=dataproc_cluster_name,
            dataproc_region=dataproc_region,
            main_python_file_uri=spark_job["MAIN_PYTHON_FILE_URI"],
            python_file_uris=spark_job["PYTHON_FILE_URIS"],
            task_timeout=task_timeout or None,
            pre_execute=pre_execute if table_to_check_payload else None,
            CONTEXT=self.dag_context,
            DATAPROC_BUCKET=dataproc_bucket,
            **args,
        )

    # -------------------------------------------------------------
    def _data_quality(
        self,
        dag: DAG,
        task_id: str,
        spark_job: Dict[str, str],
        dataproc_project_id: str,
        dataproc_cluster_name: str,
        dataproc_region: str,
        dataproc_bucket: str,
        data_quality_info: str,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        destination_table: str,
        destination_table_partition: str,
        destination_table_source_format: str,
        destination_table_flg_foto: str,
        task_timeout: int = None,
        task_group: TaskGroup = None,
    ) -> DataprocSubmitJobOperator:
        return self._submit_job(
            dag=dag,
            task_group=task_group,
            task_id=task_id,
            dataproc_project_id=dataproc_project_id,
            dataproc_cluster_name=dataproc_cluster_name,
            dataproc_region=dataproc_region,
            main_python_file_uri=spark_job["MAIN_PYTHON_FILE_URI"],
            python_file_uris=spark_job["PYTHON_FILE_URIS"],
            task_timeout=task_timeout or None,
            CONTEXT=self.dag_context,
            DATA_QUALITY_INFO=data_quality_info,
            PROJECT_ID=destination_table_project_id,
            BUCKET=destination_table_bucket,
            DATASET=destination_table_dataset,
            TABLE=destination_table,
            PARTITION=destination_table_partition,
            SOURCE_FORMAT=destination_table_source_format,
            FLG_FOTO=destination_table_flg_foto,
            DATAPROC_BUCKET=dataproc_bucket,
        )

    # -------------------------------------------------------------
    def _submit_job(
        self,
        dag: DAG,
        task_id: str,
        dataproc_project_id: str,
        dataproc_cluster_name: str,
        dataproc_region: str,
        main_python_file_uri: str,
        python_file_uris: List[str],
        task_timeout: int = None,
        pre_execute: Callable = None,
        task_group: TaskGroup = None,
        **args,
    ) -> DataprocSubmitJobOperator:
        PYSPARK_JOB = {
            "reference": {"project_id": dataproc_project_id},
            "placement": {"cluster_name": dataproc_cluster_name},
            "pyspark_job": {
                "main_python_file_uri": main_python_file_uri,
                "python_file_uris": python_file_uris,
                "args": [
                    json.dumps(
                        {
                            "ENVIRONMENT": os.getenv("env"),
                            "DAG_ID": "{{ ti.dag_id }}",
                            "RUN_ID": "{{ ti.xcom_pull(key = 'run_id') }}",
                            "TASK_ID": "{{ ti.task_id }}",
                            **args,
                        }
                    )
                ],
            },
        }

        submit_job_operator = DataprocSubmitJobOperator(
            task_id=task_id,
            project_id=dataproc_project_id,
            location=dataproc_region,
            job=PYSPARK_JOB,
            wait_timeout=task_timeout
            or (self.task_timeout.total_seconds() if self.task_timeout else None),
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
            dag=dag,
            task_group=task_group,
        )

        if pre_execute:
            submit_job_operator.pre_execute = pre_execute

        return submit_job_operator
