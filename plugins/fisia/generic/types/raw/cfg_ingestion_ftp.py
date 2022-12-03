from dataclasses import dataclass
from typing import Dict

from ..base import Base


@dataclass
class IngestionFtp(Base):
    RAW_TABLE: str
    TRD_TABLE: str
    REMOTE_PATH: str
    FILE_NAME: str
    DELIMITER: str
    EXTENSION: str
    ORIGIN_ENCODE: str
    CREDENTIAL: str
    ORIGIN_PATH: str
    FILE_INGESTION_NAME: str
    TYPE_PARTITION: str
    PARTITION: str
    ORIGIN_PARTITION: str
    MODE: str
    FLG_FOTO: str
    ACTIVE: str
    FLG_TIME: bool = False
    FORMAT_SQL_COLUMNS: Dict[str, str] = None
    INFER_SCHEMA: bool = None
