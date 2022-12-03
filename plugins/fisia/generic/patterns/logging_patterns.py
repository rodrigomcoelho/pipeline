import logging
from datetime import datetime

import pytz


class LoggingPatterns:
    """
    Class Logs Patterns

    Keep unified all the logs (Airflow and Dataproc Jobs) to use in Logs Explorer ou ELK

    >>  logger: str - Logger to be used. Default is "airflow.task" (keep using the Airflow's). To be user in DataProc, set it with another name.
    >>  identifier: str - useful to recognize your logs in different tools. Default: "##CNT_CUSTOM_LOGS##"
    >>  layer: str - log layer (RAW, TRUSTED, REFINED). Default: "RAW"
    >>  airflow_context: str - Context from the DAG. Default: "default_context"
    >>  airflow_dag_name: str - DAG Name. Default: "default_dag_name"
    """

    # -------------------------------------------------------------
    # [AIRFLOW_DEFAULT] | ##CUSTOM_LOGS## | DATA | CAMADA | REPOSITORIO | RUN_ID  |  CONTEXT  |  DAG_NAME  |  TASK_NAME  | TYPE | MESSAGE | EXCEPTION
    # [%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s

    # -------------------------------------------------------------------------------------------------------
    def __init__(
        self,
        logger: str = None,
        identifier: str = None,
        layer: str = None,
        repository: str = None,
        airflow_context: str = None,
        airflow_dag_name: str = None,
        airflow_task_name: str = None,
        airflow_run_id: str = None,
    ):
        """
        Class constructor
        """
        self.identifier = identifier or "##CNT_CUSTOM_LOGS##"
        self.layer = layer or "RAW"
        self.repository = repository
        self.airflow_run_id = airflow_run_id or "default_run_id"
        self.airflow_context = airflow_context or "default_context"
        self.airflow_dag_name = airflow_dag_name or "default_dag_name"
        self.airflow_task_name = airflow_task_name or "default_task_name"
        self.logger = logging.getLogger(logger or "CUSTOM_LOGS")

        # ** DEFAULT AIRFLOW - JUST FOR LOCAL TESTS **
        # logging.basicConfig(format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s", level=logging.INFO)

        # LOGS FROM DATAPROC/DAG PATTERNS
        if logger != "airflow.task":
            self.logger.setLevel(logging.INFO)
            self.logger.propagate = False
            self.__ch = logging.StreamHandler()
            self.__ch.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
                )
            )
            self.logger.addHandler(self.__ch)
        # ---------------------------------------------

    # -------------------------------------------------------------------------------------------------------
    def info(
        self,
        msg="",
        layer="",
        repository="",
        exception="",
        run_id="",
        context="",
        dag_name="",
        task_name="",
    ):
        """
        Trigger INFO log

        >> "Sao_Paulo timezone" is used to generate this log
        """
        self.logger.info(
            f"|{self.identifier}"
            f"|{datetime.now(tz=pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')}"
            f"|{layer or self.layer}"
            f"|{repository or self.repository}"
            f"|{run_id or self.airflow_run_id}"
            f"|{context or self.airflow_context}"
            f"|{dag_name or self.airflow_dag_name}"
            f"|{task_name or self.airflow_task_name}"
            f"|{logging.getLevelName(logging.INFO)}"
            f"|{msg}"
            f"|{exception}"
        )

    # -------------------------------------------------------------------------------------------------------
    def error(
        self,
        msg="",
        layer="",
        repository="",
        exception="",
        run_id="",
        context="",
        dag_name="",
        task_name="",
    ):
        """
        Trigger ERROR log

        >> "Sao_Paulo timezone" is used to generate this log
        """
        self.logger.error(
            f"|{self.identifier}"
            f"|{datetime.now(tz=pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')}"
            f"|{layer or self.layer}"
            f"|{repository or self.repository}"
            f"|{run_id or self.airflow_run_id}"
            f"|{context or self.airflow_context}"
            f"|{dag_name or self.airflow_dag_name}"
            f"|{task_name or self.airflow_task_name}"
            f"|{logging.getLevelName(logging.ERROR)}"
            f"|{msg}"
            f"|{exception}"
        )

    # -------------------------------------------------------------------------------------------------------
    def warning(
        self,
        msg="",
        layer="",
        repository="",
        exception="",
        run_id="",
        context="",
        dag_name="",
        task_name="",
    ):
        """
        Trigger WARNING log

        >> "Sao_Paulo timezone" is used to generate this log
        """
        self.logger.warning(
            f"|{self.identifier}"
            f"|{datetime.now(tz=pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')}"
            f"|{layer or self.layer}"
            f"|{repository or self.repository}"
            f"|{run_id or self.airflow_run_id}"
            f"|{context or self.airflow_context}"
            f"|{dag_name or self.airflow_dag_name}"
            f"|{task_name or self.airflow_task_name}"
            f"|{logging.getLevelName(logging.WARNING)}"
            f"|{msg}"
            f"|{exception}"
        )

    # -------------------------------------------------------------------------------------------------------
    def debug(
        self,
        msg="",
        layer="",
        repository="",
        exception="",
        run_id="",
        context="",
        dag_name="",
        task_name="",
    ):
        """
        Trigger DEBUG log

        >> "Sao_Paulo timezone" is used to generate this log
        """
        self.logger.debug(
            f"|{self.identifier}"
            f"|{datetime.now(tz=pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')}"
            f"|{layer or self.layer}"
            f"|{repository or self.repository}"
            f"|{run_id or self.airflow_run_id}"
            f"|{context or self.airflow_context}"
            f"|{dag_name or self.airflow_dag_name}"
            f"|{task_name or self.airflow_task_name}"
            f"|{logging.getLevelName(logging.DEBUG)}"
            f"|{msg}"
            f"|{exception}"
        )

    # -------------------------------------------------------------------------------------------------------
    def log_task_start(self, context):
        self.airflow_run_id = context["dag_run"].run_id
        self.airflow_context = context["ti"].task_id
        self.airflow_dag_name = context["ti"].dag_id
        self.airflow_task_name = context["ti"].task_id

        self.info(
            msg=f"Task [{context['ti'].task_id}] - START",
            run_id=context["dag_run"].run_id,
            task_name=context["ti"].task_id,
        )

    # -------------------------------------------------------------------------------------------------------
    def log_task_error(self, context):
        self.error(
            msg=f"Task [{context['ti'].task_id}] - FAILED",
            exception=context["exception"],
            run_id=context["dag_run"].run_id,
            task_name=context["ti"].task_id,
        )

    # -------------------------------------------------------------------------------------------------------
    def log_task_success(self, context):
        self.info(
            msg=f"Task [{context['ti'].task_id}] - SUCCESS",
            run_id=context["dag_run"].run_id,
            task_name=context["ti"].task_id,
        )

    # -------------------------------------------------------------------------------------------------------
    def log_task_retry(self, context):
        self.warning(
            msg=f"Task [{context['ti'].task_id}] - RETRY",
            run_id=context["dag_run"].run_id,
            task_name=context["ti"].task_id,
        )
