from typing import Dict, Iterable

from airflow.exceptions import AirflowException
from airflow.models import DAG, BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


class CustomBranchPythonOperator(PythonOperator, SkipMixin):
    def execute(self, context: Dict):
        enabled_tasks: Iterable[str] = super().execute(context)

        if not isinstance(enabled_tasks, Iterable) or isinstance(enabled_tasks, str):
            raise AirflowException("Branch callable must return a list of IDs")

        skipped_tasks: set = set()
        enabled_tasks = set(enabled_tasks)

        self.log.info(f"Tasks to be run {enabled_tasks}")

        this_dag: DAG = context["dag"]
        this_task: BaseOperator = context["ti"].task

        for task in [
            task
            for task in this_task.downstream_list
            if task.task_id not in enabled_tasks
        ]:
            downstream = (
                task
                if task.task_id == task.label
                else this_dag.task_group.get_child_by_label(
                    task.task_id[: -len("." + task.label)]
                )
            )

            if isinstance(downstream, TaskGroup) and downstream.has_task(this_task):
                downstream = task

            skipped_tasks.update(
                downstream if isinstance(downstream, TaskGroup) else (downstream,)
            )

        if skipped_tasks:
            self.log.info(
                f"Skipping tasks {sorted([task.task_id for task in skipped_tasks])}"
            )
            self.skip(context["dag_run"], context["ti"].execution_date, skipped_tasks)

        self.log.info("Done")
