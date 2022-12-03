import json
import os
import subprocess
from collections import ChainMap
from datetime import date, timedelta
from os.path import join
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable, Dict, List, Tuple, Union

import yaml
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
)
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions
from google.cloud.datacatalog_v1 import DataCatalogClient

from ..types.cluster import Cluster, SparkJob
from ..utils.custom_print import default_logging
from ..utils.utils_general import check_table_exists, get_data_catalog_bigquery
from .logging_patterns import LoggingPatterns

# Temporary
from .old_base_dag_patterns import BaseDagPatterns as TmpOld


class BaseDagPatterns:

    DEFAULT_PARTITION = "dat_chave"

    # Temporary
    def __new__(cls, *args, **kwargs):
        if "script_version" not in kwargs:
            obj = super().__new__(TmpOld)
            obj.__init__(*args, **kwargs)
            return obj

        return super().__new__(cls)

    # -------------------------------------------------------------
    def __init__(
        self,
        script_version: float,
        dag_context: str,
        layer: str,
        dag_name: str,
        domain: str,
        log_task_name: str = None,
        log_logger: str = None,
        log_identifier: str = None,
        log_run_id: str = None,
        task_timeout: float = None,
        log_repository: str = None,
    ) -> None:
        self.script_version = script_version
        self.dag_name = dag_name
        self.dag_context = dag_context
        self.layer = layer
        self.log_repository = log_repository
        self.domain = domain
        self.task_timeout = timedelta(seconds=task_timeout) if task_timeout else None

        self.logging = LoggingPatterns(
            logger=log_logger,
            identifier=log_identifier,
            layer=layer,
            repository=log_repository,
            airflow_run_id=log_run_id,
            airflow_context=dag_context,
            airflow_dag_name=dag_name,
            airflow_task_name=log_task_name,
        )

    # -------------------------------------------------------------
    def _load_ingestion_file(self, path: str) -> Any:
        callback = self._convert_callback(Path(path).suffix[1:])
        with open(path, "r") as file:
            return callback(file.read())

    # -------------------------------------------------------------
    def _convert_callback(self, extension: str) -> Callable[[str], Any]:
        if extension.casefold() in [x.casefold() for x in ["yml", "yaml"]]:
            return yaml.safe_load

        if extension.casefold() in [x.casefold() for x in ["json"]]:
            return json.loads

        raise TypeError(f"The '{extension}' extension is invalid!")

    # -------------------------------------------------------------
    @staticmethod
    def get_custom_variable(name: str, bu: str = None, deserialize_json: bool = True):
        value = Variable.get(name, deserialize_json=bool(deserialize_json))

        if deserialize_json and bu:
            child_var = Variable.get(
                value["ID"].format(bu.upper()),
                default_var=dict(),
                deserialize_json=True,
            )

            value = dict(ChainMap(child_var, value))

        return value

    # -------------------------------------------------------------
    @staticmethod
    def format_default_gcs_path(
        bucket: str, domain: str, table_name: str, *partitions: str
    ):
        return join(
            "gs://",
            bucket,
            domain,
            table_name,
            *[
                f"{partition.lower()}={{{partition.lower()}}}"
                for partition in partitions
            ],
        )

    # -------------------------------------------------------------
    @staticmethod
    def _execute_bash_command(command: str, raise_exception: bool = True) -> Any:
        return subprocess.run(
            command,
            check=bool(raise_exception),
            shell=True,
            executable="/bin/bash",
            capture_output=True,
            encoding="UTF-8",
        ).stdout

    # -------------------------------------------------------------
    def _checking_script_version(self):
        if self.script_version != self.TEMPLATE_VERSION:
            self.logging.warning(
                f"The current script version ({str(self.script_version)}) "
                f"does not match the default template ({str(self.TEMPLATE_VERSION)})."
            )

    # -------------------------------------------------------------
    def _validate_payload(self, payload: dict):
        default_logging("Submitted payload", "-", json.dumps(payload, indent=4))

        for _date_setup in self._squash_date_payload(payload).values():
            if _date_setup.get("START_DATE") or _date_setup.get("END_DATE"):
                assert date.fromisoformat(
                    _date_setup["END_DATE"]
                ) >= date.fromisoformat(
                    _date_setup["START_DATE"]
                ), "`END_DATE` parameter must be greater than `START_DATE`"

            if _date_setup.get("RANGE_INGESTION"):
                assert str(
                    _date_setup["RANGE_INGESTION"]
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
                self._checking_script_version(),
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
    def _date_setup(
        self,
        ti,
        dag_run,
        default_range_ingestion: int,
        data_lake_tables_name: Union[str, List[str]],
        data_lake_dependent_table_name: str = None,
    ):
        date_setup = self._define_range(
            default_range_ingestion=default_range_ingestion,
            payload=dag_run.conf,
            data_lake_tables_name=data_lake_tables_name,
            data_lake_dependent_table_name=data_lake_dependent_table_name,
            dag_name=ti.dag_id,
        )

        if data_lake_dependent_table_name:
            date_setup = {data_lake_dependent_table_name: date_setup}

        for key, value in date_setup.items():
            default_logging(key, "-", json.dumps(value, indent=4), end="\n")
            ti.xcom_push(key=f"SETUP_{key}".upper(), value=value)

    # -------------------------------------------------------------
    def _define_date_setup(
        self,
        dag: DAG,
        default_range_ingestion: int,
        data_lake_tables_name: Union[str, List[str]],
        data_lake_dependent_table_name: str = None,
        task_id: str = "define_date_setup",
        task_timeout: timedelta = None,
    ):
        return PythonOperator(
            dag=dag,
            task_id=task_id,
            do_xcom_push=False,
            python_callable=self._date_setup,
            op_kwargs=dict(
                default_range_ingestion=default_range_ingestion,
                data_lake_tables_name=data_lake_tables_name,
                data_lake_dependent_table_name=data_lake_dependent_table_name,
            ),
            execution_timeout=task_timeout or self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

    # -------------------------------------------------------------
    def _define_cluster(
        self,
        dag: DAG,
        db_type: str,
        region: str,
        suffix_cluster_name: str,
        info_dataproc: dict,
        task_id_create: str,
        task_id_delete: str,
        task_timeout: timedelta = None,
        task_group: TaskGroup = None,
        tables: list = None,
        **kwargs,
    ) -> Cluster:
        cluster_vars = Cluster(
            DB_TYPE=db_type,
            REGION=region,
            TABLES=tables,
            DATAPROC_CLUSTER_NAME=info_dataproc["DATAPROC_CLUSTER_NAME"]
            .format(suffix_cluster_name)
            .lower(),
            DATAPROC_PROJECT_ID=info_dataproc["PROJECT_ID"],
            DATAPROC_BUCKET=info_dataproc[f"INFO_{region}"]["DATAPROC_BUCKET"],
            DATAPROC_REGION=info_dataproc[f"INFO_{region}"]["DATAPROC_REGION"],
            DATAPROC_ZONE=info_dataproc[f"INFO_{region}"]["DATAPROC_ZONE"],
            DATAPROC_SERVICE_ACCOUNT=info_dataproc["DATAPROC_SERVICE_ACCOUNT"],
            DATAPROC_SERVICE_ACCOUNT_SCOPES=info_dataproc[
                "DATAPROC_SERVICE_ACCOUNT_SCOPES"
            ],
            DATAPROC_M_MACHINE_TYPE=info_dataproc["DATAPROC_M_MACHINE_TYPE"][db_type],
            DATAPROC_W_MACHINE_TYPE=info_dataproc["DATAPROC_W_MACHINE_TYPE"],
            DATAPROC_SUBNETWORK_URI=info_dataproc[f"INFO_{region}"][
                "DATAPROC_SUBNETWORK_URI"
            ],
            DATAPROC_INIT_ACTIONS_URIS=info_dataproc["DATAPROC_INIT_ACTIONS_URIS"],
            TAGS=info_dataproc["TAGS"],
            DATAPROC_AUTOSCALING_POLICY=info_dataproc[f"INFO_{region}"][
                "AUTOSCALING_POLICY"
            ],
            FLG_DELETE_CLUSTER=bool(int(info_dataproc["FLG_DELETE_CLUSTER"])),
            CLUSTER_GROUP=task_group,
            CLUSTER_CREATED=None,
            CLUSTER_DELETE=None,
            KWARGS=kwargs,
        )

        cluster_vars.CLUSTER_CREATED = DataprocClusterCreateOperator(
            task_id=task_id_create,
            cluster_name=cluster_vars.DATAPROC_CLUSTER_NAME,
            project_id=cluster_vars.DATAPROC_PROJECT_ID,
            num_workers=2,
            zone=cluster_vars.DATAPROC_ZONE,
            region=cluster_vars.DATAPROC_REGION,
            storage_bucket=cluster_vars.DATAPROC_BUCKET,
            subnetwork_uri=cluster_vars.DATAPROC_SUBNETWORK_URI,
            internal_ip_only=True,
            init_actions_uris=cluster_vars.DATAPROC_INIT_ACTIONS_URIS,
            properties={
                "dataproc:pip.packages": "google-cloud-bigquery==2.20.0,google-cloud-datacatalog==3.9.1,fs-gcsfs==1.4.1,requests==2.25.1,cx_oracle==8.2.1,slack-sdk==3.15.2,great-expectations==0.13.30,google-cloud-secret-manager==2.10.0",
                "dataproc:dataproc.conscrypt.provider.enable": "false",
                "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
                "dataproc:dataproc.logging.stackdriver.enable": "true",
                "dataproc:jobs.file-backed-output.enable": "true",
            },
            autoscaling_policy=cluster_vars.DATAPROC_AUTOSCALING_POLICY,
            use_if_exists=True,
            tags=cluster_vars.TAGS,
            master_machine_type=cluster_vars.DATAPROC_M_MACHINE_TYPE,
            worker_machine_type=cluster_vars.DATAPROC_W_MACHINE_TYPE,
            service_account=cluster_vars.DATAPROC_SERVICE_ACCOUNT,
            service_account_scopes=cluster_vars.DATAPROC_SERVICE_ACCOUNT_SCOPES,
            execution_timeout=task_timeout or self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
            dag=dag,
            task_group=task_group,
        )

        cluster_vars.CLUSTER_DELETE = (
            DataprocClusterDeleteOperator(
                task_id=task_id_delete,
                project_id=cluster_vars.DATAPROC_PROJECT_ID,
                cluster_name=cluster_vars.DATAPROC_CLUSTER_NAME,
                region=cluster_vars.DATAPROC_REGION,
                trigger_rule=TriggerRule.ALL_DONE,
                execution_timeout=task_timeout or self.task_timeout,
                on_execute_callback=self.logging.log_task_start,
                on_failure_callback=self.logging.log_task_error,
                on_success_callback=self.logging.log_task_success,
                on_retry_callback=self.logging.log_task_retry,
                dag=dag,
                task_group=task_group,
            )
            if cluster_vars.FLG_DELETE_CLUSTER
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
        spark_job: SparkJob,
        dataproc_project_id: str,
        dataproc_cluster_name: str,
        dataproc_region: str,
        dataproc_bucket: str,
        task_timeout: int = None,
        task_group: TaskGroup = None,
        table_name: str = None,
        **args,
    ) -> DataprocSubmitJobOperator:
        pre_execute = lambda context: self._table_was_selected(
            tables=table_name, context=context, launch_skip_exception=True
        )

        return self._submit_job(
            dag=dag,
            task_group=task_group,
            task_id=task_id,
            dataproc_project_id=dataproc_project_id,
            dataproc_cluster_name=dataproc_cluster_name,
            dataproc_region=dataproc_region,
            main_python_file_uri=spark_job.MAIN_PYTHON_FILE_URI,
            python_file_uris=spark_job.PYTHON_FILE_URIS,
            task_timeout=task_timeout or None,
            pre_execute=pre_execute if table_name else None,
            CONTEXT=self.dag_context,
            PLATFORM_PROJECT_ID=dataproc_project_id,
            DATAPROC_BUCKET=dataproc_bucket,
            **args,
        )

    # -------------------------------------------------------------
    def _ingestion_trusted(
        self,
        dag: DAG,
        task_id: str,
        cluster: Cluster,
        spark_job: SparkJob,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        destination_table: str,
        destination_table_partition: str,
        destination_table_type_partition: str,
        destination_table_source_format: str,
        destination_table_mode: str,
        destination_table_flg_foto: str,
        origin_table_bucket: str,
        origin_table_dataset: str,
        origin_table: str,
        create_table_if_needed: bool,
        format_sql_columns: Dict[str, str],
        path_ingestion_control_partitions: str,
        path_ingestion_control_count: str,
        task_timeout: int = None,
        task_group: TaskGroup = None,
    ):
        return self._ingestion(
            dag=dag,
            task_id=task_id,
            spark_job=spark_job,
            dataproc_project_id=cluster.DATAPROC_PROJECT_ID,
            dataproc_cluster_name=cluster.DATAPROC_CLUSTER_NAME,
            dataproc_region=cluster.DATAPROC_REGION,
            dataproc_bucket=cluster.DATAPROC_BUCKET,
            task_timeout=task_timeout,
            task_group=task_group,
            PROJECT_ID=destination_table_project_id,
            DESTINATION_BUCKET=destination_table_bucket,
            DESTINATION_DATASET=destination_table_dataset,
            DESTINATION_TABLE=destination_table,
            PARTITION=destination_table_partition,
            TYPE_PARTITION=destination_table_type_partition,
            SOURCE_FORMAT=destination_table_source_format,
            MODE=destination_table_mode,
            FLG_FOTO=destination_table_flg_foto,
            ORIGIN_BUCKET=origin_table_bucket,
            ORIGIN_DATASET=origin_table_dataset,
            ORIGIN_TABLE=origin_table,
            CREATE_TABLE_IF_NEEDED=create_table_if_needed,
            FORMAT_SQL_COLUMNS=format_sql_columns,
            PATH_INGESTION_CONTROL_PARTITIONS=path_ingestion_control_partitions,
            PATH_INGESTION_CONTROL_COUNT=path_ingestion_control_count,
        )

    # -------------------------------------------------------------
    def _data_quality(
        self,
        dag: DAG,
        task_id: str,
        spark_job: SparkJob,
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
        path_ingestion_control_partitions: str,
        path_ingestion_control_count: str,
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
            main_python_file_uri=spark_job.MAIN_PYTHON_FILE_URI,
            python_file_uris=spark_job.PYTHON_FILE_URIS,
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
            PATH_INGESTION_CONTROL_COUNT=path_ingestion_control_count,
            PATH_INGESTION_CONTROL_PARTITIONS=path_ingestion_control_partitions,
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
        post_execute: Callable = None,
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
                            "DOMAIN": self.domain,
                            "LOG_REPOSITORY": self.log_repository,
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

        if post_execute:
            submit_job_operator.post_execute = post_execute

        return submit_job_operator

    # --------------------------------------------------------------
    def _launch_pod_operator(
        self,
        dag: DAG,
        task_id: str,
        gke_project_id: str,
        gke_cluster_name: str,
        gke_region: str,
        gke_namespace: str,
        gke_service_account_name: str,
        gke_affinity: dict,
        gke_tolerations: List[dict],
        gcp_conn_id: str,
        image: str,
        labels: Dict[str, str],
        pod_name: str = None,
        task_timeout: int = None,
        pre_execute: Callable = None,
        post_execute: Callable = None,
        task_group: TaskGroup = None,
        **args,
    ) -> GKEStartPodOperator:
        gke_pod_operator = GKEStartPodOperator(
            dag=dag,
            task_group=task_group,
            task_id=task_id,
            name=pod_name or task_id,
            image=image,
            labels=labels,
            project_id=gke_project_id,
            location=gke_region,
            cluster_name=gke_cluster_name,
            namespace=gke_namespace,
            service_account_name=gke_service_account_name,
            gcp_conn_id=gcp_conn_id,
            env_vars={
                "SETUP": json.dumps(
                    {
                        "ENVIRONMENT": os.getenv("env"),
                        "DAG_ID": "{{ ti.dag_id }}",
                        "RUN_ID": "{{ ti.xcom_pull(key = 'run_id') }}",
                        "TASK_ID": "{{ ti.task_id }}",
                        "DOMAIN": self.domain,
                        "LOG_REPOSITORY": self.log_repository,
                        **args,
                    }
                )
            },
            reattach_on_restart=False,
            image_pull_policy="Always",
            is_delete_operator_pod=True,
            log_events_on_failure=True,
            affinity=gke_affinity,
            tolerations=gke_tolerations,
            execution_timeout=task_timeout or self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

        if pre_execute:
            gke_pod_operator.pre_execute = pre_execute

        if post_execute:
            gke_pod_operator.post_execute = post_execute

        return gke_pod_operator

    # -------------------------------------------------------------
    def load_data_to_bigquery(
        self,
        dag: DAG,
        project_id: str,
        bucket: str,
        dataset: str,
        table_name: str,
        partition: str = None,
        source_format: str = None,
        type_partition: str = None,
        clustering_fields: Union[str, List[str]] = None,
        expiration_ms: str = None,
        create_if_needed: bool = False,
        task_group: TaskGroup = None,
        task_timeout: timedelta = None,
    ) -> PythonOperator:
        return PythonOperator(
            dag=dag,
            task_group=task_group,
            task_id=f"trd_load_to_bq__{table_name}",
            do_xcom_push=False,
            python_callable=self._load_data_to_bigquery,
            op_kwargs=dict(
                project_id=project_id,
                bucket=bucket,
                dataset=dataset,
                table_name=table_name,
                partition=partition,
                source_format=source_format,
                type_partition=type_partition,
                clustering_fields=clustering_fields,
                expiration_ms=expiration_ms,
                create_if_needed=create_if_needed,
            ),
            execution_timeout=task_timeout or self.task_timeout,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

    # -------------------------------------------------------------
    def _load_data_to_bigquery(
        self,
        project_id: str,
        bucket: str,
        dataset: str,
        table_name: str,
        partition: str = None,
        source_format: str = None,
        type_partition: str = None,
        clustering_fields: Union[str, List[str]] = None,
        expiration_ms: str = None,
        create_if_needed: bool = False,
    ):

        # DATA CATALOG - DOMAIN
        bq_client = bigquery.Client(project_id)
        dc_client = DataCatalogClient()

        domain = self.domain.lower()

        table_exists = check_table_exists(
            bq_client=bq_client, table_id=f"{project_id}.{dataset}.{table_name}"
        )

        if table_exists:
            table_entry = get_data_catalog_bigquery(
                dc_client=dc_client,
                project_id=project_id,
                dataset=dataset,
                table_name=table_name,
            )
            domain = table_entry.labels.get("assunto_de_dados", domain).lower()

        elif bool(create_if_needed) == False:
            msg_error = f"Table `{project_id}.{dataset}.{table_name}` is not created in bigquery."
            self.logging.error(msg_error)
            raise Exception(msg_error)

        # BIGQUERY
        table_id = f"{project_id}.{dataset}.{table_name}"
        storage_path = self.format_default_gcs_path(bucket, domain, table_name)

        partition = str(partition or self.DEFAULT_PARTITION).lower()

        if (
            int(
                self._execute_bash_command(
                    f"gsutil ls -d {join(storage_path, f'{partition}=*')} | wc -l"
                )
            )
            < 1
        ):
            return self.logging.warning(f"No file in `{storage_path}` directory!")

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = source_format or "PARQUET"

        if create_if_needed:
            job_config.write_disposition, job_config.create_disposition = (
                bigquery.WriteDisposition.WRITE_TRUNCATE,
                bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )
        else:
            list(bq_client.query(f"TRUNCATE TABLE `{table_id}`"))
            job_config.write_disposition, job_config.create_disposition = (
                bigquery.WriteDisposition.WRITE_EMPTY,
                bigquery.CreateDisposition.CREATE_NEVER,
            )

        if partition:
            hive_config = HivePartitioningOptions()
            hive_config.mode = "AUTO"
            hive_config.source_uri_prefix = storage_path

            job_config.hive_partitioning = hive_config
            job_config.time_partitioning = bigquery.TimePartitioning(
                field=partition,
                type_=getattr(
                    bigquery.TimePartitioningType, str(type_partition or "DAY").upper()
                ),
                expiration_ms=expiration_ms or None,
            )

        clustering_fields = (
            set(clustering_fields)
            if isinstance(clustering_fields, list)
            else {clustering_fields}
        )
        clustering_fields = [col.lower() for col in clustering_fields if col]
        job_config.clustering_fields = clustering_fields or None

        load_job = bq_client.load_table_from_uri(
            join(storage_path, "*"), table_id, job_config=job_config, project=project_id
        )

        self.logging.info(f"Performing load on table `{table_id}`")
        load_job.result()

        table = bq_client.get_table(table_id)

        if not table_exists:
            table.labels = {"assunto_de_dados": domain}
            table = bq_client.update_table(table, ["labels"])

        self.logging.info(f"Loaded {str(table.num_rows)} rows in table `{table_id}`")

        self.logging.info(
            "TABLE SCHEMA:"
            + "\n\n"
            + ":: TABULAR REPRESENTATION:"
            + "\n\n"
            + "\n".join(["\t".join([row.name, row.field_type]) for row in table.schema])
            + "\n\n\n"
            + ":: JSON REPRESENTATION:"
            + "\n\n"
            + json.dumps([col.to_api_repr() for col in table.schema], indent=4)
            + "\n\n\n"
        )

    # -------------------------------------------------------------
    def _format_range(
        self, start_date: str = None, end_date: str = None, range_ingestion: int = None
    ) -> Tuple[bool, Dict[str, str]]:
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

        return True, dict(
            START_DATE=start_date.isoformat(), END_DATE=end_date.isoformat()
        )

    # -------------------------------------------------------------
    def _format_range_by_tables(
        self,
        tables_name: List[str],
        setup_by_table: Dict[str, Dict[str, str]],
    ) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
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
        bigquery_client: bigquery.Client = None,
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
            for _, row in bigquery_client.query(query).to_dataframe.iterrows()
        }

    # -------------------------------------------------------------
    def _squash_date_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            **payload.get("DATE_SETUP", {}),
            "*": {
                key: value
                for key, value in payload.items()
                if key in ("START_DATE", "END_DATE", "RANGE_INGESTION")
            },
        }

    # -------------------------------------------------------------
    def _define_range(
        self,
        default_range_ingestion: int,
        data_lake_tables_name: Union[str, List[str]],
        payload: Dict[str, Any] = dict(),
        dag_name: str = None,
        data_lake_dependent_table_name: str = None,
        bigquery_client: bigquery.Client = None,
    ) -> Dict[str, Dict[str, str]]:
        tables_range = ChainMap()
        data_lake_tables_name = tables_not_defined = (
            [data_lake_tables_name]
            if isinstance(data_lake_tables_name, str)
            else list(data_lake_tables_name)
        )

        if payload:
            tables_defined, tables_not_defined = self._format_range_by_tables(
                tables_name=tables_not_defined,
                setup_by_table=self._squash_date_payload(payload),
            )
            tables_range.maps.append(tables_defined or dict())

        if False and bigquery_client and tables_not_defined and data_lake_tables_name:
            tables_defined, tables_not_defined = self._format_range_by_tables(
                tables_name=tables_not_defined,
                setup_by_table=self._get_setup_date_bq(
                    data_lake_tables_name=data_lake_tables_name,
                    dag_name=dag_name,
                    data_lake_dependent_table_name=data_lake_dependent_table_name,
                    bigquery_client=bigquery_client,
                ),
            )
            tables_range.maps.append(tables_defined or dict())

        if tables_not_defined:
            tables_defined, tables_not_defined = self._format_range_by_tables(
                tables_name=tables_not_defined,
                setup_by_table={"*": {"RANGE_INGESTION": default_range_ingestion}},
            )
            tables_range.maps.append(tables_defined or dict())

        return dict(tables_range)

    # ---------------------------------------------------------------
