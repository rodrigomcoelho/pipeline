import json
from collections import ChainMap
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib import parse

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from ..utils.custom_print import default_logging


class CustomTriggerDagRunOperator(TriggerDagRunOperator):
    def __init__(
        self,
        trigger_dag_id: str,
        python_callable: Callable[[Any], Tuple[Dict, List[Dict]]],
        op_kwargs: Optional[Dict] = None,
        conf: Optional[Dict] = None,
        poke_interval: int = 60,
        xcom_results_key: str = None,
        **kwargs,
    ) -> None:

        super().__init__(
            trigger_dag_id=trigger_dag_id,
            conf=conf,
            poke_interval=poke_interval,
            **kwargs,
        )

        self.python_callable = python_callable
        self.op_kwargs = op_kwargs
        self.xcom_results_key = xcom_results_key or trigger_dag_id.upper()

    def execute(self, context: Dict):
        dict_config, list_config = self.python_callable(context, **self.op_kwargs)
        self.execution_results = {
            State.SUCCESS: 0,
            State.FAILED: 0,
            "EXECUTIONS": list(),
        }

        default_config = dict(ChainMap(dict_config or {}, self.conf or {}))
        base_url = parse.urljoin(conf.get("webserver", "base_url"), "graph")
        params_url = {"dag_id": self.trigger_dag_id, "execution_date": None}

        default_logging("*" * 100)
        default_logging(("  " + self.trigger_dag_id + "  ").center(100, "*").upper())
        default_logging("*" * 100)
        default_logging("")
        default_logging("SETUP:", json.dumps(default_config, indent=4))

        for config in list_config:
            self.conf = dict(ChainMap(config or {}, default_config))

            self.execution_date = timezone.utcnow()
            params_url["execution_date"] = self.execution_date.isoformat()
            url = base_url + "?" + parse.urlencode(params_url)

            trigger_run_id = DagRun.generate_run_id(
                DagRunType.MANUAL, self.execution_date
            )

            default_logging("")
            default_logging("")
            default_logging("*" * 80)
            default_logging("*" * 80)
            default_logging("#", "Starting", self.trigger_dag_id)
            default_logging("", "→", "RUN ID:", trigger_run_id)
            default_logging("", "→", "URL:", url)
            default_logging("", "→", "SETUP:", json.dumps(self.conf))
            default_logging("")

            try:
                super().execute(context)
                execution_state = State.SUCCESS
                default_logging("")
                default_logging("#", State.SUCCESS.upper())

            except AirflowException as err:
                if err.args[0].casefold() == "Task received SIGTERM signal".casefold():
                    raise err

                execution_state = State.FAILED
                default_logging("")
                default_logging("#", State.FAILED.upper(), "-->", err.args[0])

            self.execution_results[execution_state] += 1
            self.execution_results["EXECUTIONS"] += (
                dict(
                    execution_state=execution_state,
                    trigger_run_id=trigger_run_id,
                    config=config,
                    url=url,
                ),
            )

            self.xcom_push(
                context=context,
                key=self.xcom_results_key,
                value=self.execution_results["EXECUTIONS"],
            )

        default_logging("*" * 100)
        default_logging(("  " + self.trigger_dag_id + "  ").center(100, "*").upper())
        default_logging("*" * 100)

        default_logging("")
        default_logging("")
        default_logging(
            "RESULT:",
            State.FAILED.capitalize()
            if self.execution_results[State.FAILED]
            else State.SUCCESS.capitalize(),
        )
        default_logging(
            "  *", f"{State.SUCCESS.upper()}:", self.execution_results[State.SUCCESS]
        )
        default_logging(
            "  *", f"{State.FAILED.upper()}:", self.execution_results[State.FAILED]
        )

        default_logging("")
        default_logging("")
        default_logging("DEFAULT SETUP:", json.dumps(default_config, indent=4))

        default_logging("")
        default_logging("")
        default_logging("EXECUTIONS:")
        default_logging("")
        for execution in self.execution_results["EXECUTIONS"]:
            default_logging("", "→", "RUN_ID:", execution["trigger_run_id"])
            default_logging("   ", "-", "STATE:", execution["execution_state"].lower())
            default_logging("   ", "-", "SETUP:", json.dumps(execution["config"]))
            default_logging("")

        if self.execution_results[State.FAILED]:
            raise AirflowException
