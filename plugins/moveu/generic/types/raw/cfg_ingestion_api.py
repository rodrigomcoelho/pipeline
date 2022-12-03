from dataclasses import dataclass
from typing import Dict

from ..base import Base


@dataclass
class IngestionAPI(Base):
    RAW_TABLE: str
    TRD_TABLE: str
    FORMAT_OUTPUT: str
    ACTIVE: str
    FLG_FOTO: str
    ENDPOINT_CONFIG: dict
    PARTITION: str
    TRANSIENT_TYPE_PARTITION: str
    FORMAT_SQL_COLUMNS: Dict[str, str] = None
