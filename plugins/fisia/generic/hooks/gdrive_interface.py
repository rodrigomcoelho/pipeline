import logging
from typing import List, Union

from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

from ..patterns.logging_patterns import LoggingPatterns


class GoogleDriveHookCustom(GoogleDriveHook):

    # -------------------------------------------------------------
    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        logger: Union[LoggingPatterns, logging.Logger] = logging,
    ):
        super().__init__(gcp_conn_id=gcp_conn_id)
        self.logger = logger

    # -------------------------------------------------------------
    def get_multiples_file_id(self, folder_id: str, file_name: str) -> List[str]:
        query = f"mimeType != 'application/vnd.google-apps.folder'"
        query += f" and name = '{file_name}'" if file_name != "*" else ""
        query += f" and parents in '{folder_id}'"

        files = (
            self.get_conn()
            .files()
            .list(
                q=query,
                spaces="drive",
                corpora="allDrives",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                fields="files(id, mimeType, name)",
                orderBy="modifiedTime desc",
            )
            .execute(num_retries=self.num_retries)
        )

        if files["files"]:
            for file in files["files"]:
                self.logger.info(f"*** Arquivo encontrado: {file['name']}")

            return [
                {"id": file["id"], "mime_type": file["mimeType"], "name": file["name"]}
                for file in files["files"]
            ]

        return []
