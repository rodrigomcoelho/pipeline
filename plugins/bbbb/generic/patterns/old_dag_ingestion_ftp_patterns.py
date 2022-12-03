# ========================================================================= #
#   This module is deprecated. Please use ./dag_ingestion_ftp_patterns.py   #
# ========================================================================= #

import gzip
import io
import os
import zipfile
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path

from airflow.models import DAG
from airflow.models.connection import Connection
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

from .old_base_dag_patterns import BaseDagPatterns


class DagIngestionFtpPatterns(BaseDagPatterns):

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
    def define_clusters(
        self,
        dag: DAG,
        region: str = "US",
        task_id_create: str = "create_dataproc_cluster",
        task_id_delete: str = "delete_dataproc_cluster",
    ):
        self.cluster = self._define_cluster(
            dag=dag,
            db_type="INGESTION_FTP",
            region=region,
            tables=[file["TABLE"] for file in self.dag_files],
            suffix_cluster_name=self.dag_context,
            info_dataproc=self.info_dataproc,
            info_ingestion=self.info_ingestion,
            task_id_create=task_id_create,
            task_id_delete=task_id_delete,
        )

    # -------------------------------------------------------------
    @staticmethod
    def lastsuffix(file):
        return Path(file).suffixes[-1].lower()

    # -------------------------------------------------------------
    @staticmethod
    def get_files(file_name: str, start_date, end_date):
        files = []
        dt = datetime.strptime(start_date, "%Y-%m-%d")
        delta = (datetime.strptime(end_date, "%Y-%m-%d") - dt).days + 1
        for _ in range(delta):
            archive = file_name.format(datetime.strftime(dt, "%Y%m%d"))
            files.append(archive)
            dt = dt + timedelta(days=1)
        if files == []:
            files.append(file_name.format((datetime.now()).strftime("%Y%m%d")))
        return files

    # ----------------------------------------------------------------------------

    def _get_file_sftp(
        self,
        ti,
        sftp_conn: dict,
        file: dict,
        process_date: str,
        destination_bucket: str,
        start_date: str,
        end_date: str,
    ):
        if start_date == None:
            start_date = process_date
        if end_date == None:
            end_date = process_date
        self.logging.info(f"start_date = {start_date}")
        self.logging.info(f"end_date = {end_date}")
        file_name = file["FILE_NAME"]
        extension = file["EXTENSION"]
        origin_encode = file["ORIGIN_ENCODE"]
        remote_path = file["REMOTE_PATH"]
        credential = file["CREDENTIAL"]
        table_name = file["TABLE"]
        self.logging.info(f"file name : {file_name}")

        c = Connection(
            conn_id=credential,
            conn_type=sftp_conn[credential]["CONN_TYPE"],
            description=sftp_conn[credential]["DESCRIPTION"],
            host=sftp_conn[credential]["HOST"],
            login=sftp_conn[credential]["LOGIN"],
            password=sftp_conn[credential]["PASSWORD"],
            extra=sftp_conn[credential]["EXTRA"],
        )
        os.environ[f"AIRFLOW_CONN_{c.conn_id.upper()}"] = c.get_uri()
        sftp_hook = SFTPHook(c.conn_id)
        # self.logging.info(f"range_begin : {range_begin_from_today}")
        # self.logging.info(f"range_end : {range_end_from_today}")
        self.logging.info(f"file_name : {file_name}")

        list_files = self.get_files(file_name, start_date, end_date)
        list_read = []

        self.logging.info(f"LISTA DE ARQUIVOS A SEREM RECEBIDOS : {list_files}")
        temp_path = "/tmp/FTP/originals"
        self.logging.info(f"criando estrura de pastas {temp_path}")
        os.system(f"mkdir -p {temp_path}")
        self.logging.info(f"temp_path : {temp_path}")
        os.system(f"gsutil rm gs://{destination_bucket}/FTP/convert/{table_name}/*")

        # download file to local directory using sftp
        for file in list_files:
            origin_path = join(remote_path, file)
            self.logging.info(f"ORIGIN : {origin_path} DESTINY: {temp_path}/{file}")
            base_file = Path(file).stem
            full_temp_path = join(temp_path, file)
            extension_file = self.lastsuffix(full_temp_path)
            sftp_hook.retrieve_file(origin_path, join(temp_path, file))

            os.system(f"ls -ltr {temp_path}")

            if extension_file == ".zip":
                self.unzip_and_convert_file(full_temp_path, extension, origin_encode)
            else:
                self.convert_file(full_temp_path, extension, origin_encode)

            # copy converted file to gs
            destination_path = join(
                "gs://",
                destination_bucket,
                "FTP/convert",
                table_name,
                f"{base_file}.{extension}.gz",
            )
            self.logging.info(f"destination_path : {destination_path}")
            os.system(
                f"gsutil -m cp {temp_path}/{base_file}.{extension}.gz {destination_path}"
            )
            self.logging.info(
                f"gsutil -m cp {temp_path}/{base_file}.{extension}.gz {destination_path}"
            )
            list_read.append({"name": table_name, "mime_type": extension})
            if extension_file == ".zip":
                os.remove(f"{temp_path}/{base_file}.zip")
            os.remove(f"{temp_path}/{base_file}.{extension}.gz")
        ti.xcom_push(key=table_name, value=list_read)

    # -------------------------------------------------------------
    def unzip_and_convert_file(self, zip_file: str, extension: str, origin_encode: str):

        base_file = Path(zip_file).stem
        temp_path = os.path.dirname(zip_file)
        gz_file = f"{temp_path}/{base_file}.{extension}.gz"
        handle_out_file = gzip.open(gz_file, "wb")

        with zipfile.ZipFile(zip_file) as handle_zip:
            list_of_files = handle_zip.namelist()
            for file in list_of_files:
                self.logging.info(f"archive in zip : {file}")
                if self.lastsuffix(file) == ".csv":
                    convert = True
                if self.lastsuffix(file) == f".{extension}":
                    with handle_zip.open(file, mode="r") as handle_file:
                        for line in io.TextIOWrapper(
                            handle_file, encoding=origin_encode
                        ):
                            # Converte linha a linha para o padrao utf-8 e envia para buffer para ser comprimido
                            if convert:
                                converted_line = bytes(line, encoding="utf-8")
                            else:
                                converted_line = line
                            handle_out_file.write(converted_line)
        handle_zip.close()
        handle_out_file.close()

    # --------------------------------------------------------------
    def convert_file(self, file: str, extension: str, origin_encode: str):

        gz_file = f"{file}.gz"
        handle_out_file = gzip.open(gz_file, "wb")
        if self.lastsuffix(file) == ".csv":
            convert = True
        if self.lastsuffix(file) == f".{extension}":
            with open(file, mode="r", encoding=origin_encode) as handle_file:
                for line in handle_file:
                    # Converte linha a linha para o padrao utf-8 e envia para buffer para ser comprimido
                    if convert:
                        converted_line = bytes(line, encoding="utf-8")
                    else:
                        converted_line = line
                    _ = handle_out_file.write(converted_line)
        handle_file.close()
        handle_out_file.close()

    # ---------------------------------------------------------------

    def convert_to_utf8(
        self,
        dag: DAG,
        process_date: str,
        task_id: str,
        file: dict,
        destination_bucket: str,
        sftp_conn: dict,
    ):

        python_operator = PythonOperator(
            task_id=task_id,
            provide_context=True,
            python_callable=(self._get_file_sftp),
            op_kwargs={
                "file": file,
                "process_date": process_date,
                "destination_bucket": destination_bucket,
                "sftp_conn": sftp_conn,
                "start_date": "{% if 'START_DATE' in dag_run.conf %}{{dag_run.conf['START_DATE']}}{% else %}"
                + process_date
                + "{% endif %}",
                "end_date": "{% if 'END_DATE' in dag_run.conf %}{{dag_run.conf['END_DATE']}}{% else %}"
                + process_date
                + "{% endif %}",
            },
            # do_xcom_push = False,
            dag=dag,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

        python_operator.pre_execute = lambda context: self._table_was_selected(
            tables=file["TABLE"], context=context, launch_skip_exception=True
        )

        return python_operator

    # ---------------------------------------------------------------
    def ingestion_file_task(
        self,
        dag: DAG,
        file: dict,
        destination_table_bucket: str,
        destination_table_dataset: str,
        destination_table_project_id: str,
        process_date: str,
    ):
        return self._ingestion(
            dag=dag,
            task_id=f"update_table__{file['TABLE']}",
            spark_job=self.cluster["SPARK_JOBS"]["INGESTION"],
            dataproc_project_id=self.cluster["DATAPROC_PROJECT_ID"],
            dataproc_cluster_name=self.cluster["DATAPROC_CLUSTER_NAME"],
            dataproc_region=self.cluster["DATAPROC_REGION"],
            dataproc_bucket=self.cluster["DATAPROC_BUCKET"],
            LAKE_PROJECT_ID=destination_table_project_id,
            ORIGIN_PATH=join(
                "gs://",
                self.info_ingestion["ORIGINAL_BUCKET"],
                file["ORIGIN_PATH"],
                file["TABLE"],
            ),
            DESTINATION_BUCKET=destination_table_bucket,
            DATASET=destination_table_dataset,
            TABLE=file["TABLE"],
            DELIMITER=file["DELIMITER"],
            FILES="{{ task_instance.xcom_pull(key = '" + file["TABLE"] + "') }}",
            PARTITION=file["PARTITION"],
            ORIGIN_PARTITION=file["ORIGIN_PARTITION"],
            MODE=file["MODE"],
            FLG_FOTO=file["FLG_FOTO"],
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
        destination_table_partition: str,
        destination_table_flg_foto: str,
    ):
        return self._data_quality(
            dag=dag,
            task_id=f"data_quality__{destination_table}",
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
            destination_table_partition=destination_table_partition,
            destination_table_source_format=None,
            destination_table_flg_foto=destination_table_flg_foto,
        )
