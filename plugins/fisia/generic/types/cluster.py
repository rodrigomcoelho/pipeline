from dataclasses import dataclass, field
from typing import Any, Dict, List, Union

from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
)
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from .base import Base


@dataclass
class SparkJob(Base):
    MAIN_PYTHON_FILE_URI: str
    PYTHON_FILE_URIS: List[str] = field(default_factory=lambda: [])


@dataclass
class Cluster(Base):
    DB_TYPE: str
    REGION: str
    DATAPROC_CLUSTER_NAME: str
    DATAPROC_PROJECT_ID: str
    DATAPROC_BUCKET: str
    DATAPROC_REGION: str
    DATAPROC_ZONE: str
    DATAPROC_SERVICE_ACCOUNT: str
    DATAPROC_SERVICE_ACCOUNT_SCOPES: List[str]
    DATAPROC_M_MACHINE_TYPE: str
    DATAPROC_W_MACHINE_TYPE: str
    DATAPROC_SUBNETWORK_URI: str
    DATAPROC_INIT_ACTIONS_URIS: str
    TAGS: List[str]
    DATAPROC_AUTOSCALING_POLICY: str
    FLG_DELETE_CLUSTER: bool

    CLUSTER_GROUP: TaskGroup = None
    CLUSTER_CREATED: DataprocClusterCreateOperator = None
    CLUSTER_DELETE: Union[DummyOperator, DataprocClusterDeleteOperator] = None

    TABLES: str = None
    KWARGS: Dict[str, Any] = None
