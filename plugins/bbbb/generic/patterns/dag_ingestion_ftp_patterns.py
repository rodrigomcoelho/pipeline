import gzip
import io
import os
import re
import zipfile
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path
from typing import Any, Dict, List

from airflow.models import DAG
from airflow.models.connection import Connection
from airflow.operators.python_operator import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

from ..templates import versioning
from ..types.cluster import SparkJob
from ..types.raw.cfg_ingestion_ftp import IngestionFtp
from .base_dag_patterns import BaseDagPatterns

# Temporary
from .old_dag_ingestion_ftp_patterns import DagIngestionFtpPatterns as TmpOld


class DagIngestionFtpPatterns(BaseDagPatterns):

    # Versão do template de criação das DAGs
    TEMPLATE_VERSION: float = versioning.RAW_INGESTION_FTP

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
        path_ingestion_config: dict,
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
            dag_name=dag_name,
            domain=domain,
            log_run_id=log_run_id,
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
    def _adapter_result(self, content_file: List[Dict[str, Any]]) -> List[IngestionFtp]:
        return [
            IngestionFtp(**table) for table in content_file if table["ACTIVE"] == "1"
        ]

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
            db_type="INGESTION_FTP",
            region=region,
            tables=[file.RAW_TABLE for file in self.cfg_ingestion],
            suffix_cluster_name=self.dag_context,
            info_dataproc=self.info_dataproc,
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
        file: IngestionFtp,
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
        file_name = file.FILE_NAME
        extension = file.EXTENSION
        origin_encode = file.ORIGIN_ENCODE
        remote_path = file.REMOTE_PATH
        credential = file.CREDENTIAL
        table_name = file.RAW_TABLE
        flg_time = file.FLG_TIME
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
            base_file = Path(file).stem
            self.logging.info(f"BASE_FILE : {base_file}")
            extension_file = self.lastsuffix(file)
            final_list = []
            if flg_time:
                self.logging.info(f"ENTROU NO FLAG_TIME")
                fs = base_file + "_*" + extension_file
                self.logging.info(f"FS : {fs}")
                # Lista o diretorio
                for l in sftp_hook.list_directory(f"{remote_path}"):
                    regex = base_file + r"_[0-9]{6}" + extension_file
                    # Encontra todos os arquivos que possuem 6 numeros apos o _
                    result = re.findall(regex, l)

                    if len(result) != 0:
                        final_list.append(l)
                        sftp_hook.retrieve_file(
                            join(remote_path, l), join(temp_path, l)
                        )
            else:
                final_list.append(file)
                self.logging.info(f"NAO ENTROU NO FLAG_TIME")
                self.logging.info(f"FILE : {file}")
                sftp_hook.retrieve_file(join(remote_path, file), join(temp_path, file))

            # self.logging.info(f"ORIGIN : {origin_path} DESTINY: {dest_path}")
            self.logging.info("LIST DIR")
            os.system(f"ls -ltr {temp_path}/")

            for final_file in final_list:

                full_temp_path = join(temp_path, final_file)
                if extension_file == ".zip":
                    self.unzip_and_convert_file(
                        full_temp_path, extension, origin_encode
                    )
                else:
                    self.convert_file(full_temp_path, extension, origin_encode)
            # copy converted file to gs
            for final_file in final_list:
                base = Path(final_file).stem
                gz_file = f"{base}.{extension}.gz"
                destination_path = join(
                    "gs://", destination_bucket, "FTP/convert", table_name, gz_file
                )
                self.logging.info(
                    f"gsutil -m cp {temp_path}/{gz_file} {destination_path}"
                )
                os.system(f"gsutil -m cp {temp_path}/{gz_file} {destination_path}")
                if extension_file == ".zip":
                    os.remove(f"{temp_path}/{base}.zip")
                os.remove(f"{temp_path}/{gz_file}")
        list_read.append({"name": table_name, "mime_type": extension})
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
        file: IngestionFtp,  # ---------------- Object? IngestionFtp
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
            dag=dag,
            on_execute_callback=self.logging.log_task_start,
            on_failure_callback=self.logging.log_task_error,
            on_success_callback=self.logging.log_task_success,
            on_retry_callback=self.logging.log_task_retry,
        )

        python_operator.pre_execute = lambda context: self._table_was_selected(
            tables=file.RAW_TABLE, context=context, launch_skip_exception=True
        )

        return python_operator

    # ---------------------------------------------------------------
    def raw_update_table(
        self,
        dag: DAG,
        destination_table: str,
        file: IngestionFtp,
        destination_table_project_id: str,
        destination_table_bucket: str,
        destination_table_dataset: str,
        process_date: str,
        path_ingestion_control_partitions: str,
        path_ingestion_control_count: str,
        destination_trd_table_dataset: str,
        create_table_if_needed: bool,
    ):
        return self._ingestion(
            dag=dag,
            task_id=f"update_table__{destination_table}",
            spark_job=self.raw_spark_job,
            dataproc_project_id=self.cluster.DATAPROC_PROJECT_ID,
            dataproc_cluster_name=self.cluster.DATAPROC_CLUSTER_NAME,
            dataproc_region=self.cluster.DATAPROC_REGION,
            dataproc_bucket=self.cluster.DATAPROC_BUCKET,
            LAKE_PROJECT_ID=destination_table_project_id,
            ORIGIN_PATH=join(
                "gs://",
                self.info_raw_ingestion["ORIGINAL_BUCKET"],
                file.ORIGIN_PATH,
                file.RAW_TABLE,
            ),
            DESTINATION_BUCKET=destination_table_bucket,
            DATASET=destination_table_dataset,
            TABLE=file.RAW_TABLE,
            DELIMITER=file.DELIMITER,
            FILES="{{ task_instance.xcom_pull(key = '" + file.RAW_TABLE + "') }}",
            PARTITION=file.PARTITION,
            ORIGIN_PARTITION=file.ORIGIN_PARTITION,
            MODE=file.MODE,
            FLG_FOTO=file.FLG_FOTO,
            PATH_INGESTION_CONTROL_PARTITIONS=path_ingestion_control_partitions,
            PATH_INGESTION_CONTROL_COUNT=path_ingestion_control_count,
            TRD_TABLE=file.TRD_TABLE,
            TRD_DATASET=destination_trd_table_dataset,
            CREATE_TABLE_IF_NEEDED=create_table_if_needed,
            INFER_SCHEMA=file.INFER_SCHEMA,
        )

    # -------------------------------------------------------------
    def trusted_update_table(
        self,
        dag: DAG,
        destination_table: str,
        file: IngestionFtp,
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
            destination_table_partition=file.PARTITION,
            destination_table_type_partition=file.TYPE_PARTITION,
            destination_table_source_format=None,
            destination_table_mode=file.MODE,
            destination_table_flg_foto=file.FLG_FOTO,
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
        file: IngestionFtp,
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
            destination_table_partition=file.PARTITION,
            destination_table_source_format=None,
            destination_table_flg_foto=file.FLG_FOTO,
            path_ingestion_control_partitions=path_ingestion_control_partitions,
            path_ingestion_control_count=path_ingestion_control_count,
        )
