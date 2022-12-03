from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule


class DagPatterns:

    # -------------------------------------------------------------
    def __init__(self, dag_context, dag_info, dag_connections, dag_tables):
        self.dag_context = dag_context
        self.dag_info = dag_info
        self.dag_connections = dag_connections
        self.dag_tables = dag_tables

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
        return DummyOperator(task_id="complete_process", dag=dag)

    # -------------------------------------------------------------
    @staticmethod
    def update_tables(
        dag,
        dataproc_spark,
        project_id,
        cluster_name,
        region,
        url,
        user,
        password,
        query,
        bucket,
        dataset,
        table,
        mode,
        bq_writedisposition,
        project,
        partition,
        type_partition,
        clustered,
        source_format,
        expiration_ms,
        flg_foto,
        **kwargs,
    ):
        PYSPARK_JOB = {
            "reference": {"project_id": project_id},
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": dataproc_spark,
                "args": [
                    url,
                    user,
                    password,
                    query,
                    bucket,
                    dataset,
                    table,
                    mode,
                    bq_writedisposition,
                    project,
                    partition,
                    type_partition,
                    clustered,
                    source_format,
                    expiration_ms,
                    flg_foto,
                ],
            },
        }

        return DataprocSubmitJobOperator(
            task_id=f"update_table_{table}",
            project_id=project_id,
            location=region,
            job=PYSPARK_JOB,
            dag=dag,
        )

    # -------------------------------------------------------------
    def define_clusters(self, dag):
        cluster_vars = {}

        for cluster in {
            (
                table["DB_TYPE"],
                self.dag_connections[table["DB_TYPE"]][table["DB_OWNER"]]["REGION"],
            )
            for table in self.dag_tables
            if table["ACTIVE"] == "1"
        }:
            db_type = cluster[0]
            region = cluster[1]

            cluster_key = f"{db_type}_{region}"

            cluster_vars[cluster_key] = {
                "DB_TYPE": db_type,
                "REGION": region,
                "DATAPROC_CLUSTER_NAME": self.dag_info["DATAPROC_CLUSTER_NAME"]
                .format(f"{db_type}-{region}-{self.dag_context}")
                .lower(),
                "DATAPROC_METADATA": self.dag_info["DATAPROC_METADATA"],
                "DATAPROC_SERVICE_ACCOUNT": self.dag_info["DATAPROC_SERVICE_ACCOUNT"],
                "DATAPROC_SPARK_JOB_NAME": self.dag_info["DATAPROC_SPARK_JOB_NAME"],
                "DATAPROC_W_MACHINE_TYPE": self.dag_info["DATAPROC_W_MACHINE_TYPE"],
                "DATAPROC_PROJECT_ID": self.dag_info["PROJECT_ID"],
                "FLG_DELETE_CLUSTER": int(self.dag_info["FLG_DELETE_CLUSTER"]),
                "DATAPROC_SPARK_JOB_PATH": self.dag_info["DATAPROC_SPARK_JOB_PATH"][
                    db_type
                ],
                "DATAPROC_M_MACHINE_TYPE": self.dag_info["DATAPROC_M_MACHINE_TYPE"][
                    db_type
                ],
                "TAGS": self.dag_info["TAGS"],
                "DATAPROC_AUTOSCALING_POLICY": self.dag_info[f"INFO_{region}"][
                    "AUTOSCALING_POLICY"
                ],
                "DATAPROC_REGION": self.dag_info[f"INFO_{region}"]["DATAPROC_REGION"],
                "DATAPROC_BUCKET": self.dag_info[f"INFO_{region}"]["DATAPROC_BUCKET"],
                "DATAPROC_SUBNETWORK_URI": self.dag_info[f"INFO_{region}"][
                    "DATAPROC_SUBNETWORK_URI"
                ],
                "DATAPROC_ZONE": self.dag_info[f"INFO_{region}"]["DATAPROC_ZONE"],
                "CLUSTER_CREATED": "",
                "CLUSTER_DELETE": "",
            }

            cluster_vars[cluster_key][
                "CLUSTER_CREATED"
            ] = DataprocClusterCreateOperator(
                task_id=f"create_dataproc_cluster_{cluster_key}",
                cluster_name=cluster_vars[cluster_key]["DATAPROC_CLUSTER_NAME"],
                project_id=cluster_vars[cluster_key]["DATAPROC_PROJECT_ID"],
                num_workers=2,
                zone=cluster_vars[cluster_key]["DATAPROC_ZONE"],
                region=cluster_vars[cluster_key]["DATAPROC_REGION"],
                storage_bucket=cluster_vars[cluster_key]["DATAPROC_BUCKET"],
                subnetwork_uri=cluster_vars[cluster_key]["DATAPROC_SUBNETWORK_URI"],
                internal_ip_only=True,
                init_actions_uris=[
                    "gs://dataplatorm-dataproc-init-actions/scripts/dataproc/dataproc-install-libs.sh"
                ],
                properties={
                    "dataproc:pip.packages": "google-cloud-bigquery==2.20.0,fs-gcsfs==1.4.1,requests==2.25.1,cx_oracle==8.2.1",
                    "dataproc:dataproc.conscrypt.provider.enable": "false",
                },
                autoscaling_policy=cluster_vars[cluster_key][
                    "DATAPROC_AUTOSCALING_POLICY"
                ],
                use_if_exists=True,
                tags=cluster_vars[cluster_key]["TAGS"],
                master_machine_type=cluster_vars[cluster_key][
                    "DATAPROC_M_MACHINE_TYPE"
                ],
                worker_machine_type=cluster_vars[cluster_key][
                    "DATAPROC_W_MACHINE_TYPE"
                ],
                service_account=cluster_vars[cluster_key]["DATAPROC_SERVICE_ACCOUNT"],
                dag=dag,
            )

            cluster_vars[cluster_key]["CLUSTER_DELETE"] = (
                DataprocClusterDeleteOperator(
                    task_id=f"delete_dataproc_cluster_{cluster_key}",
                    project_id=cluster_vars[cluster_key]["DATAPROC_PROJECT_ID"],
                    cluster_name=cluster_vars[cluster_key]["DATAPROC_CLUSTER_NAME"],
                    region=cluster_vars[cluster_key]["DATAPROC_REGION"],
                    trigger_rule=TriggerRule.ALL_DONE,
                    dag=dag,
                )
                if cluster_vars[cluster_key]["FLG_DELETE_CLUSTER"]
                else DummyOperator(
                    task_id=f"delete_dataproc_cluster_{cluster_key}", dag=dag
                )
            )

        self.clusters = cluster_vars
