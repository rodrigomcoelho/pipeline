from dataclasses import dataclass
from typing import Dict


@dataclass
class IngestionGcs:
    RAW_TABLE: str
    TRD_TABLE: str
    SOURCE_TYPE: str
    FILE_NAME: str
    ACTIVE: str
    FLG_FOTO: str
    PARTITION: str
    ORIGIN_PARTITION: str = None
    QUERY: str = None
    FORMAT_SQL_COLUMNS: Dict[str, str] = None
