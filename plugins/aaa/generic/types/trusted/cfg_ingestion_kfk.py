from dataclasses import dataclass
from typing import List

from ..base import Base


@dataclass
class MergeOrderedBy(Base):
    FIELD: str
    IS_ASCENDING: bool = False


@dataclass
class DateFilter(Base):
    FIELD: str
    RANGE_BEGIN_FROM_TODAY: int
    RANGE_END_FROM_TODAY: int


@dataclass
class Workflow(Base):
    TYPE: str
    BU: str = None
    LAYER: str = None
    SOURCE_BUCKET: str = None
    SOURCE_TABLE: str = None
    TEMP_TABLE: str = None
    FILTER: DateFilter = None
    QUERY: str = None
    MERGE_KEYS: List[str] = None
    ORDERED_BY: List[MergeOrderedBy] = None

    def __post_init__(self):
        if isinstance(self.FILTER, dict):
            self.FILTER = DateFilter(**self.FILTER)

        if isinstance(self.ORDERED_BY, list):
            self.ORDERED_BY = [
                MergeOrderedBy(**ordered_by)
                if isinstance(ordered_by, dict)
                else ordered_by
                for ordered_by in self.ORDERED_BY
            ]


@dataclass
class IngestionKfk(Base):
    TRD_TABLE: str
    MODE: str
    PARTITION: str
    TYPE_PARTITION: str
    CLUSTERED: str
    SOURCE_FORMAT: str
    FLG_FOTO: str
    ACTIVE: str
    WORKFLOW: List[Workflow]

    def __post_init__(self):
        self.WORKFLOW = [
            Workflow(**workflow) if isinstance(workflow, dict) else workflow
            for workflow in self.WORKFLOW
        ]
