import sys
from os.path import join
from pathlib import Path

from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
)
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule

# Libs auxiliares
PATH_IMPORT = Variable.get("IMPORT_PATH")
sys.path.append(PATH_IMPORT)
from libs.generic.hooks.gcs_interface import GoogleDriveToGCSHookCustom
from libs.generic.hooks.gdrive_interface import GoogleDriveHookCustom


class DagIngestionFilePatterns:

    # -------------------------------------------------------------
    def __init__(self, dag_context, dag_info, dag_files):
        self.dag_context = dag_context
        self.dag_info = dag_info
        self.dag_tables = dag_files

    # -------------------------------------------------------------
    @staticmethod
    def created_start_job(dag):
        return DummyOperator(task_id="start_job", dag=dag)

    # -------------------------------------------------------------
    @staticmethod
    def complete_process(dag):
        return DummyOperator(task_id="complete_process", dag=dag)

    # -------------------------------------------------------------
    def _exec_search(self, ti, folder_id, file_name, idx):
        files = GoogleDriveHookCustom(
            gcp_conn_id=self.dag_info["INGESTION_FILE"]["GCLOUD_CONN_ID"]
        ).get_multiples_file_id(folder_id=folder_id, file_name=file_name)

        if len(files) < 1:
            raise ValueError("*** GDrive - File not found ***")

        ti.xcom_push(key=f"files_listed__{idx}", value=files)

    # ------------------------------------------------------
    def _move_to_gcs(self, ti, file_ingestion, idx):
        files = ti.xcom_pull(key=f"files_listed__{idx}")

        gcs_interface = GoogleDriveToGCSHookCustom(
            gcp_conn_id=self.dag_info["INGESTION_FILE"]["GCLOUD_CONN_ID"]
        )

        for file in files:
            if file["mime_type"] in ["application/vnd.google-apps.spreadsheet"]:
                gcs_interface.move_google_spreadsheet_to_gcs(
                    spreadsheet_id=file["id"],
                    spreadsheet_name=file["name"],
                    spreadsheet_pages=file_ingestion["SPREADSHEET_PAGES"]
                    if type(file_ingestion["SPREADSHEET_PAGES"]) == list
                    else [file_ingestion["SPREADSHEET_PAGES"]],
                    bucket_name=self.dag_info["INGESTION_FILE"]["ORIGINAL_BUCKET"],
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
                    bucket_name=self.dag_info["INGESTION_FILE"]["ORIGINAL_BUCKET"],
                    object_name=join(
                        file_ingestion["FOLDER_PATH_FROM_INPUTBI"],
                        Path(file["name"]).stem,
                        file_ingestion["TABLE"],
                    ),
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
        )

        self.cluster_delete = (
            DataprocClusterDeleteOperator(
                task_id=f"delete_dataproc_cluster_{region}",
                project_id=self._dataproc_project_id,
                cluster_name=self._cluster_name,
                region=self._dataproc_region,
                trigger_rule=TriggerRule.ALL_DONE,
                dag=dag,
            )
            if int(self.dag_info["FLG_DELETE_CLUSTER"])
            else DummyOperator(task_id=f"delete_dataproc_cluster_{region}", dag=dag)
        )

    # -------------------------------------------------------------
    def checking_files(self, index, file, dag):
        return PythonOperator(
            task_id=f"checking_files__{file['TABLE']}",
            provide_context=True,
            python_callable=self._exec_search,
            op_kwargs={
                "folder_id": file["FOLDER_ID"],
                "file_name": file["FILE_NAME"],
                "idx": index,
            },
            do_xcom_push=False,
            dag=dag,
        )

    # -------------------------------------------------------------
    def copy_gdrive_to_gcs(self, index, file, dag):
        return PythonOperator(
            task_id=f"copy_gdrive_to_gcs__{file['TABLE']}",
            provide_context=True,
            python_callable=self._move_to_gcs,
            op_kwargs={"file_ingestion": file, "idx": index},
            do_xcom_push=False,
            dag=dag,
        )

    # -------------------------------------------------------------
    def ingestion_file_task(
        self, index, file, destination_bucket, dataset, lake_project_id, dag
    ):
        PYSPARK_JOB = {
            "reference": {"project_id": self._dataproc_project_id},
            "placement": {"cluster_name": self._cluster_name},
            "pyspark_job": {
                "main_python_file_uri": self.dag_info["DATAPROC_SPARK_JOB_PATH"][
                    "INGESTION_FILE"
                ],
                "args": [
                    lake_project_id,
                    join(
                        "gs://",
                        self.dag_info["INGESTION_FILE"]["ORIGINAL_BUCKET"],
                        file["FOLDER_PATH_FROM_INPUTBI"],
                        Path(file["FILE_NAME"]).stem,
                        file["TABLE"],
                    ),
                    destination_bucket,
                    dataset,
                    file["TABLE"],
                    file["DELIMITER"],
                    str(
                        file["SPREADSHEET_PAGES"]
                        if type(file["SPREADSHEET_PAGES"]) == list
                        else [file["SPREADSHEET_PAGES"]]
                    ),
                    "{{ task_instance.xcom_pull(key = '"
                    + f"files_listed__{index}"
                    + "') }}",
                ],
            },
        }

        return DataprocSubmitJobOperator(
            task_id=f"ingestion_file__{file['TABLE']}",
            project_id=self._dataproc_project_id,
            location=self._dataproc_region,
            job=PYSPARK_JOB,
            dag=dag,
        )
