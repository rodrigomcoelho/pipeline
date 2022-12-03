from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class DagPatternsTransfer:

    # -------------------------------------------------------------
    def __init__(self, dag_context, dag_info, dag_files):
        self.dag_context = dag_context
        self.dag_info = dag_info
        self.dag_files = dag_files

    # -------------------------------------------------------------
    @staticmethod
    def created_start_job(dag):
        return PythonOperator(
            task_id="start_job",
            provide_context=True,
            python_callable=lambda: True,
            dag=dag,
        )

    # -------------------------------------------------------------
    @staticmethod
    def complete_process(dag):
        return DummyOperator(
            task_id="complete_process", dag=dag, trigger_rule=TriggerRule.ALL_DONE
        )

    # -------------------------------------------------------------
    def _checking_exist(self, project_id, dataset_id, table_id, **kwargs):
        client = bigquery.Client()
        try:
            return bool(client.get_table(f"{project_id}.{dataset_id}.{table_id}"))
        except NotFound:
            return False

    def checking(self, dag, task_id, project_id, dataset_id, table_id):
        return PythonOperator(
            task_id=task_id,
            dag=dag,
            python_callable=lambda ti, **kwargs: ti.xcom_push(
                key=table_id, value=self._checking_exist(**kwargs)
            ),
            op_kwargs={
                "project_id": project_id,
                "table_id": table_id,
                "dataset_id": dataset_id,
            },
        )

    # -------------------------------------------------------------
    def branching_process(self, dag, tables, table_exist, table_not_exist):
        return BranchPythonOperator(
            dag=dag,
            task_id="branching_process",
            python_callable=lambda ti: table_not_exist
            if False in [ti.xcom_pull(key=table) for table in tables]
            else table_exist,
        )

    # -------------------------------------------------------------
    def transform_and_create_table(
        self,
        dag,
        task_id,
        origin,
        dataset,
        table,
        mode,
        bq_writedisposition,
        project,
        bucket,
        partition,
        partition_origin,
        type_partition,
        clustered,
        source_format,
        flg_foto,
        table_exists,
        **kwargs,
    ):
        PYSPARK_JOB = {
            "reference": {"project_id": self._dataproc_project_id},
            "placement": {"cluster_name": self._cluster_name},
            "pyspark_job": {
                "main_python_file_uri": self.dag_info["DATAPROC_SPARK_JOB_PATH"][
                    "INGESTION_DATA_TRANSFER"
                ],
                "args": [
                    origin,
                    dataset,
                    table,
                    mode,
                    bq_writedisposition,
                    project,
                    bucket,
                    partition,
                    partition_origin,
                    type_partition,
                    clustered,
                    source_format,
                    flg_foto,
                    table_exists,
                ],
            },
        }

        return DataprocSubmitJobOperator(
            task_id=task_id,
            project_id=self._dataproc_project_id,
            location=self._dataproc_region,
            job=PYSPARK_JOB,
            dag=dag,
        )

    # -------------------------------------------------------------

    def define_clusters(self, dag, region="US"):
        self._dataproc_project_id = self.dag_info["PROJECT_ID"]
        self._dataproc_region = self.dag_info[f"INFO_{region}"]["DATAPROC_REGION"]
        self._cluster_name = (
            self.dag_info["DATAPROC_CLUSTER_NAME"].format(f"{self.dag_context}").lower()
        )

        self.cluster_created = DataprocClusterCreateOperator(
            task_id=f"create_dataproc_cluster_{region}",
            cluster_name=self._cluster_name,
            project_id=self._dataproc_project_id,
            num_workers=2,
            zone=self.dag_info[f"INFO_{region}"]["DATAPROC_ZONE"],
            region=self._dataproc_region,
            storage_bucket=self.dag_info[f"INFO_{region}"]["DATAPROC_BUCKET"],
            subnetwork_uri=self.dag_info[f"INFO_{region}"]["DATAPROC_SUBNETWORK_URI"],
            internal_ip_only=True,
            init_actions_uris=[
                "gs://dataplatorm-dataproc-init-actions/scripts/dataproc/dataproc-install-libs.sh"
            ],
            properties={
                "dataproc:pip.packages": "google-cloud-bigquery==2.20.0,fs-gcsfs==1.4.1,requests==2.25.1",
                "dataproc:dataproc.conscrypt.provider.enable": "false",
            },
            autoscaling_policy=self.dag_info[f"INFO_{region}"]["AUTOSCALING_POLICY"],
            use_if_exists=True,
            tags=self.dag_info["TAGS"],
            master_machine_type=self.dag_info["DATAPROC_M_MACHINE_TYPE"][
                "INGESTION_FILE"
            ],
            worker_machine_type=self.dag_info["DATAPROC_W_MACHINE_TYPE"],
            service_account=self.dag_info["DATAPROC_SERVICE_ACCOUNT"],
            dag=dag,
            trigger_rule=TriggerRule.ONE_SUCCESS,
        )

        self.cluster_delete = (
            DataprocClusterDeleteOperator(
                task_id=f"delete_dataproc_cluster_{region}",
                project_id=self._dataproc_project_id,
                cluster_name=self._cluster_name,
                region=self._dataproc_region,
                trigger_rule=TriggerRule.NONE_SKIPPED,
                dag=dag,
            )
            if int(self.dag_info["FLG_DELETE_CLUSTER"])
            else DummyOperator(
                task_id=f"delete_dataproc_cluster_{region}",
                trigger_rule=TriggerRule.NONE_SKIPPED,
                dag=dag,
            )
        )
