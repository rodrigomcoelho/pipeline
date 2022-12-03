from dataclasses import dataclass
from typing import Dict

from ..base import Base


@dataclass
class IngestionTables(Base):
    DB_TYPE: str
    DB_OWNER: str
    RAW_TABLE: str
    TRD_TABLE: str
    QUERY: str
    MODE: str
    BQ_WRITEDISPOSITION: str
    PARTITION: str
    TYPE_PARTITION: str
    CLUSTERED: str
    SOURCE_FORMAT: str
    EXPIRATION_MS: str
    FLG_FOTO: str
    ACTIVE: str
    FORMAT_SQL_COLUMNS: Dict[str, str] = None
