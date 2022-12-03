import logging
from os.path import dirname, join, realpath
from re import search
from typing import Tuple

from .versioning import *


# -------------------------------------------------------------
def read_file(file_name: str) -> str:
    with open(realpath(join(dirname(__file__), file_name))) as file:
        return file.read()


# -------------------------------------------------------------
def get_template(layer: str, dag_name: str) -> Tuple[float, str]:
    layer, dag_name = layer.lower(), dag_name.lower()

    if layer in ["raw"]:
        if search(r"_tbl$", dag_name):
            return RAW_INGESTION_TABLE, read_file("dag_template_raw_ingestion_table.py")

        if search(r"_gdr$", dag_name):
            return RAW_INGESTION_GDR, read_file("dag_template_raw_ingestion_gdr.py")

        if search(r"_ftp$", dag_name):
            return RAW_INGESTION_FTP, read_file("dag_template_raw_ingestion_ftp.py")

        if search(r"_gcs$", dag_name):
            return RAW_INGESTION_GCS, read_file("dag_template_raw_ingestion_gcs.py")

        if search(r"_api$", dag_name):
            return RAW_INGESTION_API, read_file("dag_template_raw_ingestion_api.py")

        if search(r"_[0-9]{3}$", dag_name):
            logging.warning(
                f"The `{dag_name}` dag does not have the ingestion type specification\n"
            )
            return None, None

    if layer in ["trusted"]:
        if search(r"_kfk$", dag_name):
            return TRD_INGESTION_KFK, read_file("dag_template_trd_ingestion_kfk.py")
        else:
            logging.warning(
                f"The `{dag_name}` dag does not have the ingestion type specification\n"
            )
            return None, None

    if layer in ["refined"]:
        return REF_INGESTION, read_file("dag_template_ref_ingestion.py")

    raise Exception(
        f"There is no template defined on layer `{layer}` for dag `{dag_name}`"
    )
