import json
import re
from collections import defaultdict
from datetime import date
from glob import glob
from os.path import join
from pathlib import Path
from typing import Dict, List

import yaml
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import DAG, DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from dateutil.relativedelta import relativedelta

from ..operators.custom_trigger_dag_run_operator import CustomTriggerDagRunOperator
from ..utils.custom_print import default_logging
from .logging_patterns import LoggingPatterns


class HistoryLoadPatterns:

    XCOM_LOAD_SETUP_KEY = "LOAD_SETUP"
    XCOM_DATE_SETUP_KEY = "DATE_SETUP"
    XCOM_EXECUTIONS_KEY = lambda dag_name: str(dag_name).upper()

    # -------------------------------------------------------------
    def __init__(
        self,
        layer: str,
        dag_name: str,
        dag_context: str,
        path_ingestion_config: str,
        default_cutting_date: date = date(2012, 1, 1),
        default_granularity: int = 12,
    ) -> None:

        self.cfg_ingestion = self._load_cfg_ingestion(path_ingestion_config)

        self.default_granularity = default_granularity
        self.default_cutting_date = default_cutting_date

        self.logger = LoggingPatterns(
            layer=layer, airflow_context=dag_context, airflow_dag_name=dag_name
        )

    # -------------------------------------------------------------
    def _check_table_rules(self, table):
        return bool(
            table.get("RAW_TABLE")
            and table.get("QUERY")
            and table.get("ACTIVE") == "1"
            and table.get("FLG_FOTO") == "1"
            and re.search(
                r"{(START_(DATE|DATETIME))}[\s\S]*{(END_(DATE|DATETIME))}",
                table["QUERY"],
                re.IGNORECASE,
            )
        )

    # -------------------------------------------------------------
    def _load_cfg_ingestion(self, path_ingestion_config: str) -> Dict[str, List]:
        cfg_ingestion = defaultdict(list)
        path_ingestion_config = join(path_ingestion_config, "*.yaml")
        for file_name, dag_name in [
            (file_name, Path(file_name).stem)
            for file_name in glob(path_ingestion_config)
        ]:
            with open(file_name, "r") as file:
                for table in yaml.safe_load(file.read()):
                    if self._check_table_rules(table):
                        cfg_ingestion[dag_name].append(table["RAW_TABLE"])

        return cfg_ingestion

    # -------------------------------------------------------------
    def _split_range_date(
        self, start_date: date, end_date: date, granularity: int
    ) -> List[Dict[str, str]]:
        next_date = start_date + relativedelta(months=granularity)

        if next_date >= end_date:
            return [
                {"START_DATE": start_date.isoformat(), "END_DATE": end_date.isoformat()}
            ]

        return [
            {"START_DATE": start_date.isoformat(), "END_DATE": next_date.isoformat()}
        ] + self._split_range_date(next_date, end_date, granularity)

    # -------------------------------------------------------------
    def _start_process(self, ti, dag_run) -> None:
        tables = dag_run.conf.get("TABLES")

        if not tables:
            raise ValueError(
                "It is mandatory to define the TABLES parameter in the payload when starting the DAG!"
            )

        if not isinstance(tables, (str, list)):
            raise ValueError(
                "The only data types accepted in the TABLES parameter are `string` and `list`!"
            )

        if isinstance(tables, str) and tables != "*":
            raise ValueError('The only string accepted in the TABLES parameter is "*"!')

        tables = set(tables)
        load_setup = (
            dict(self.cfg_ingestion)
            if tables == set("*")
            else {
                dag: list(tables & set(table_list))
                for dag, table_list in self.cfg_ingestion.items()
            }
        )

        date_setup = self._split_range_date(
            start_date=date.fromisoformat(
                dag_run.conf.get("START_DATE", self.default_cutting_date.isoformat())
            ),
            end_date=date.fromisoformat(
                dag_run.conf.get("END_DATE", date.today().isoformat())
            ),
            granularity=int(dag_run.conf.get("GRANULARITY", self.default_granularity)),
        )

        ti.xcom_push(key=HistoryLoadPatterns.XCOM_LOAD_SETUP_KEY, value=load_setup)
        ti.xcom_push(key=HistoryLoadPatterns.XCOM_DATE_SETUP_KEY, value=date_setup)

        default_logging(json.dumps(load_setup, indent=4, sort_keys=True))
        default_logging(json.dumps(date_setup, indent=4, sort_keys=True))

    # -------------------------------------------------------------
    def _table_was_selected(self, ti, dag_name: str, table_name: str = None) -> None:
        tables = ti.xcom_pull(key=HistoryLoadPatterns.XCOM_LOAD_SETUP_KEY).get(dag_name)
        if not tables or (table_name and table_name not in tables):
            raise AirflowSkipException
        return tables

    # -------------------------------------------------------------
    def _define_table_status(self, ti, dag_name: str, table_name: str) -> None:
        self._table_was_selected(ti, dag_name, table_name)

        dag_executions: List[Dict] = ti.xcom_pull(
            key=HistoryLoadPatterns.XCOM_EXECUTIONS_KEY(dag_name)
        )

        if not dag_executions:
            raise AirflowSkipException

        default_logging("*" * 100)
        default_logging(
            ("  " + dag_name.upper() + " - " + table_name.upper() + "  ").center(
                100, "*"
            )
        )
        default_logging("*" * 100)

        has_error = None
        for execution in dag_executions:
            dag_run = DagRun.find(dag_id=dag_name, run_id=execution["trigger_run_id"])[
                0
            ]
            task_instances = [
                x
                for x in dag_run.get_task_instances()
                if re.search(re.escape(table_name) + "$", x.task_id, re.IGNORECASE)
            ]

            default_logging("", "→", "EXECUTION ID:", execution["trigger_run_id"])
            default_logging("   ", "-", "DATE SETUP:", json.dumps(execution["config"]))
            default_logging("   ", "-", "PAYLOAD:", json.dumps(dag_run.conf))
            default_logging("   ", "-", "URL:", execution["url"])
            default_logging("   ", "-", "TASKS:")

            if task_instances:
                for task in task_instances:
                    has_error = has_error or (task.state == State.FAILED)
                    default_logging(
                        "      ", "-", task.state.upper(), "→", task.task_id
                    )
            else:
                has_error = True
                default_logging("      ", "-", "NOT FOUND!")

            default_logging("")
            default_logging("")

        if has_error:
            raise AirflowException

    # -------------------------------------------------------------
    def _start_process_operator(self, dag: DAG) -> PythonOperator:
        return PythonOperator(
            task_id="start_process",
            dag=dag,
            python_callable=self._start_process,
            on_execute_callback=self.logger.log_task_start,
            on_failure_callback=self.logger.log_task_error,
            on_success_callback=self.logger.log_task_success,
            on_retry_callback=self.logger.log_task_retry,
        )

    # -------------------------------------------------------------
    def _dag_task_group(self, dag: DAG, group_id: str) -> TaskGroup:
        return TaskGroup(
            group_id=group_id,
            dag=dag,
            prefix_group_id=True,
            ui_color="#E6E6E6",
            ui_fgcolor="#000",
        )

    # -------------------------------------------------------------
    def _trigger_dag_operator(
        self, dag: DAG, task_group: TaskGroup, dag_name: str
    ) -> CustomTriggerDagRunOperator:
        return CustomTriggerDagRunOperator(
            task_id=dag_name,
            dag=dag,
            task_group=task_group,
            trigger_dag_id=dag_name,
            wait_for_completion=True,
            poke_interval=int(2 * 60),
            python_callable=lambda context, dag_name: (
                {"TABLES": self._table_was_selected(context["ti"], dag_name)},
                context["ti"].xcom_pull(key=HistoryLoadPatterns.XCOM_DATE_SETUP_KEY),
            ),
            op_kwargs=dict(dag_name=dag_name),
            xcom_results_key=HistoryLoadPatterns.XCOM_EXECUTIONS_KEY(dag_name),
            on_execute_callback=self.logger.log_task_start,
            on_failure_callback=self.logger.log_task_error,
            on_success_callback=self.logger.log_task_success,
            on_retry_callback=self.logger.log_task_retry,
        )

    # -------------------------------------------------------------
    def _status_table_operator(
        self, dag: DAG, task_group: TaskGroup, dag_name: str, table_name: str
    ) -> PythonOperator:
        return PythonOperator(
            task_id=table_name,
            dag=dag,
            task_group=task_group,
            trigger_rule=TriggerRule.ALL_DONE,
            python_callable=self._define_table_status,
            op_kwargs=dict(dag_name=dag_name, table_name=table_name),
            on_execute_callback=self.logger.log_task_start,
            on_failure_callback=self.logger.log_task_error,
            on_success_callback=self.logger.log_task_success,
            on_retry_callback=self.logger.log_task_retry,
        )

    # -------------------------------------------------------------
    def structure_process(self, dag: DAG) -> None:
        start_process = self._start_process_operator(dag=dag)

        for dag_name, table_list in self.cfg_ingestion.items():
            tg_dag = self._dag_task_group(dag=dag, group_id=dag_name.upper())

            op_trigger_dag = self._trigger_dag_operator(
                dag=dag, task_group=tg_dag, dag_name=dag_name
            )

            for table_name in table_list:
                op_status_table = self._status_table_operator(
                    dag=dag, task_group=tg_dag, dag_name=dag_name, table_name=table_name
                )

                start_process >> op_trigger_dag >> op_status_table
