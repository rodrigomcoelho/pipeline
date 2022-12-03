import csv
import logging
from os.path import join
from tempfile import NamedTemporaryFile
from typing import Union

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook

from ..patterns.logging_patterns import LoggingPatterns


class GoogleDriveToGCSHookCustom(GoogleBaseHook):

    # -------------------------------------------------------------
    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        logger: Union[LoggingPatterns, logging.Logger] = logging,
    ):
        super().__init__(gcp_conn_id=gcp_conn_id)
        self.logger = logger
        self.gdrive_hook = GoogleDriveHook(gcp_conn_id=self.gcp_conn_id)
        self.gsheet_hook = GSheetsHook(gcp_conn_id=self.gcp_conn_id)
        self.gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)

    # -------------------------------------------------------------
    def move_file_to_gcs(
        self, file_id: str, file_name: str, bucket_name: str, object_name: str
    ) -> None:
        self._clear_folder(bucket_name=bucket_name, object_name=object_name)

        with self.gcs_hook.provide_file_and_upload(
            bucket_name=bucket_name, object_name=join(object_name, file_name)
        ) as gcs_file:
            self.gdrive_hook.download_file(file_id=file_id, file_handle=gcs_file)

        self.logger.info(
            f"[{file_name} :: {file_id}] moved to [{join('gs://', bucket_name, object_name, file_name)}]"
        )

    # -------------------------------------------------------------
    def move_google_spreadsheet_to_gcs(
        self,
        spreadsheet_id: str,
        spreadsheet_name: str,
        spreadsheet_pages: Union[str, list],
        range_sheets: Union[str, list],
        bucket_name: str,
        object_name: str,
        output_delimiter: str = "|",
    ) -> None:
        self._clear_folder(bucket_name=bucket_name, object_name=object_name)

        if not spreadsheet_pages or spreadsheet_pages[0] in ["", "*"]:
            spreadsheet_pages = self.gsheet_hook.get_sheet_titles(spreadsheet_id)

        if not range_sheets or range_sheets in [None, ""]:
            range_sheets = ["A1" for i in spreadsheet_pages]
        if len(spreadsheet_pages) != len(range_sheets):
            raise Exception("spreedsheet_pages =! range_sheets")

        for page, range_sheet in zip(spreadsheet_pages, range_sheets):
            self.logger.info(f"{page}!{range_sheet}")
            print(f"{page}!{range_sheet}")
            # Gsheet nao pega planilha inteira usando A1
            if range_sheet == "A1":
                range_ = page
            else:
                range_ = f"{page}!{range_sheet}"
            print(f"range = {range_}")
            with NamedTemporaryFile("w+") as csv_file:
                csv.writer(csv_file, delimiter=output_delimiter or "|").writerows(
                    self.gsheet_hook.get_values(
                        spreadsheet_id=spreadsheet_id,
                        range_=range_,
                        value_render_option="UNFORMATTED_VALUE",
                    )
                )
                csv_file.flush()
                self.gcs_hook.upload(
                    bucket_name=bucket_name,
                    object_name=join(object_name, f"{page}.csv"),
                    filename=csv_file.name,
                )

        self.logger.info(
            f"[{spreadsheet_name} :: {spreadsheet_id}] moved to [{join('gs://', bucket_name, object_name)}]"
        )

    # -------------------------------------------------------------
    def _clear_folder(self, bucket_name: str, object_name: str) -> None:
        for file in self.gcs_hook.list(bucket_name, prefix=object_name):
            self.gcs_hook.delete(bucket_name=bucket_name, object_name=file)
