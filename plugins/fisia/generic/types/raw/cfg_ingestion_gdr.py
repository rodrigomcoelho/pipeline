from dataclasses import dataclass
from typing import Dict, List, Union

from ..base import Base


@dataclass
class IngestionGdr(Base):
    RAW_TABLE: str
    TRD_TABLE: str
    FOLDER_PATH_FROM_INPUTBI: str
    FOLDER_ID: str
    FILE_NAME: str
    SPREADSHEET_PAGES: Union[str, List[str]]
    DELIMITER: str
    ACTIVE: str
    RANGE_SHEETS: Union[str, List[str]] = None
    FORMAT_SQL_COLUMNS: Dict[str, str] = None
