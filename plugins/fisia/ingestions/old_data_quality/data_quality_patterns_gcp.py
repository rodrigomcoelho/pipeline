# ====================================================================================== #
#   This module is deprecated. Please use ../data_quality/data_quality_patterns_gcp.py   #
# ====================================================================================== #

import json
from datetime import datetime
from os.path import join
from textwrap import dedent
from typing import Callable, Dict, List
from uuid import uuid4

import pandas as pd
from expectation import Expectation
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from old_ingestion_patterns_gcp import IngestionPatterns
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import NumericType
from slack_sdk import WebClient


class DataQualityPatterns:

    # -------------------------------------------------------------
    def __init__(self, df: DataFrame) -> None:
        self.__df = SparkDFDataset(df)
        self.__schema = df.schema.fields

        self.__expects: Dict[str, Expectation] = dict()

    # -------------------------------------------------------------
    @staticmethod
    def get_expectations(
        table: str, callback_client: Callable[[str], pd.DataFrame]
    ) -> List[dict]:
        query = dedent(
            f"""
                SELECT
                    tb.nom_tabela AS table,
                    tb.nom_dataset AS layer,
                    tb.nom_dag AS dag,
                    tb.nom_repositorio AS repository,
                    tb.cod_slack_lista AS slack_id,
                    def.cod_configuracao_expectativa AS id_config_expectation,
                    def.nom_expectativa AS expectation,
                    def.des_colunas AS columns,
                    custom.cod_lista_parametro AS required_params,
                    def.cod_lista_parametro AS submitted_params
                FROM
                    `data_quality.cnt_tab_data_quality_definicao_expectativas` def
                INNER JOIN
                    `data_quality.cnt_tab_data_quality_expectativas_customizadas` custom
                ON
                    def.nom_expectativa = custom.nom_expectativa
                INNER JOIN
                    `ingestion_control.cnt_tab_informacoes_tabelas` tb
                ON
                    def.nom_tabela = tb.nom_tabela
                WHERE
                    tb.nom_tabela IN ('{table}')
                    AND custom.flg_regra_ativa IS TRUE
                    AND def.flg_regra_ativa IS TRUE
            """
        )

        return json.loads(callback_client(query).to_json(orient="records"))

    # -------------------------------------------------------------
    @staticmethod
    def format_params(required_param: list, submitted_param: list):
        params = dict()
        for key in [key["nom_chave"] for key in required_param]:
            value = [
                param["des_valor"]
                for param in submitted_param
                if param["nom_chave"] == key
            ]
            if len(value) != 1:
                raise ValueError(f"Submitted param for key `{key}` is invalid!")

            params[key] = value[0]

        return params

    # -------------------------------------------------------------
    def expect_row_count_to_equal_origin(
        self, *args, expectation_config: str, origin_count: int, **kwargs
    ) -> None:
        if expectation_config not in self.__expects:
            self.__expects[expectation_config] = Expectation(
                expectation_config=expectation_config,
                expectation_name=self.expect_row_count_to_equal_origin.__name__,
                output_element_count=["result", "observed_value"],
                output_origin_count=["expectation_config", "kwargs", "value"],
                output_is_success=["success"],
                expectation=self.__df.expect_table_row_count_to_equal,
            )

        self.__expects[expectation_config].execute(value=origin_count)

    # -------------------------------------------------------------
    def expect_column_values_to_not_be_null(
        self, *columns: str, expectation_config: str, **kwargs
    ) -> None:
        if expectation_config not in self.__expects:
            self.__expects[expectation_config] = Expectation(
                expectation_config=expectation_config,
                expectation_name=self.expect_column_values_to_not_be_null.__name__,
                output_columns=["expectation_config", "kwargs", "column"],
                output_element_count=["result", "element_count"],
                output_unexpected_count=["result", "unexpected_count"],
                output_is_success=["success"],
                expectation=self.__df.expect_column_values_to_not_be_null,
            )

        self.__expects[expectation_config].execute(
            columns=columns or [col.name for col in self.__schema]
        )

    # -------------------------------------------------------------
    def expect_column_values_non_negative_number(
        self, *columns: str, expectation_config: str, **kwargs
    ) -> None:
        if expectation_config not in self.__expects:
            self.__expects[expectation_config] = Expectation(
                expectation_config=expectation_config,
                expectation_name=self.expect_column_values_non_negative_number.__name__,
                output_columns=["expectation_config", "kwargs", "column"],
                output_element_count=["result", "element_count"],
                output_unexpected_count=["result", "unexpected_count"],
                output_missing_count=["result", "missing_count"],
                output_is_success=["success"],
                expectation=self.__df.expect_column_values_to_be_between,
            )

        self.__expects[expectation_config].execute(
            columns=columns
            or [
                col.name
                for col in self.__schema
                if isinstance(col.dataType, NumericType)
            ],
            min_value=0,
        )

    # -------------------------------------------------------------
    def expect_column_values_non_zero_number(
        self, *columns: str, expectation_config: str, **kwargs
    ) -> None:
        if expectation_config not in self.__expects:
            self.__expects[expectation_config] = Expectation(
                expectation_config=expectation_config,
                expectation_name=self.expect_column_values_non_zero_number.__name__,
                output_columns=["expectation_config", "kwargs", "column"],
                output_element_count=["result", "element_count"],
                output_unexpected_count=["result", "unexpected_count"],
                output_missing_count=["result", "missing_count"],
                output_is_success=["success"],
                expectation=self.__df.expect_column_values_to_not_be_in_set,
            )

        self.__expects[expectation_config].execute(
            columns=columns
            or [
                col.name
                for col in self.__schema
                if isinstance(col.dataType, NumericType)
            ],
            value_set=[0],
        )

    # -------------------------------------------------------------
    def expect_column_values_not_empty(
        self, *columns: str, expectation_config: str, **kwargs
    ) -> None:
        if expectation_config not in self.__expects:
            self.__expects[expectation_config] = Expectation(
                expectation_config=expectation_config,
                expectation_name=self.expect_column_values_not_empty.__name__,
                output_columns=["expectation_config", "kwargs", "column"],
                output_element_count=["result", "element_count"],
                output_unexpected_count=["result", "unexpected_count"],
                output_missing_count=["result", "missing_count"],
                output_is_success=["success"],
                expectation=self.__df.expect_column_values_to_not_match_regex,
            )

        self.__expects[expectation_config].execute(
            columns=columns
            or [
                col.name
                for col in self.__schema
                if not isinstance(col.dataType, NumericType)
            ],
            regex="^ {0,}$",
        )

    # -------------------------------------------------------------
    def expect_column_values_to_be_unique(
        self, *columns: str, expectation_config: str, **kwargs
    ) -> None:
        if not columns:
            raise ValueError("`columns` parameter is missing")

        if expectation_config not in self.__expects:
            self.__expects[expectation_config] = Expectation(
                expectation_config=expectation_config,
                expectation_name=self.expect_column_values_to_be_unique.__name__,
                output_columns=["expectation_config", "kwargs", "column_list"],
                output_element_count=["result", "element_count"],
                output_unexpected_count=["result", "unexpected_count"],
                output_missing_count=["result", "missing_count"],
                output_is_success=["success"],
                expectation=self.__df.expect_compound_columns_to_be_unique,
            )

        self.__expects[expectation_config].execute(
            columns=[
                [column] if type(column) == str else list(column) for column in columns
            ]
        )

    # -------------------------------------------------------------
    def execute_validation(self):
        self.__result = self.__df.validate()
        (
            self.__result_execution,
            self.__result_expectation,
        ) = Expectation.adapter_execution(
            json=self.__result, expectations=self.__expects
        )

        return (self.__result_execution, self.__result_expectation, self.__result)

    # -------------------------------------------------------------
    def write_results(
        self,
        spark_session: SparkSession,
        run_id: str,
        project_id: str,
        dataset: str,
        table: str,
        temp_bucket: str,
        data_quality_bucket: str,
        executed_at: datetime = datetime.utcnow(),
        execution_id: str = str(uuid4()),
    ) -> str:
        result_execution = Expectation.df_execution(
            spark_session=spark_session,
            data=self.__result_execution,
            execution_id=execution_id,
            table=table,
            executed_at=executed_at,
            run_id=run_id,
            json=json.dumps(json.loads(str(self.__result))),
        )

        result_expectation = Expectation.df_expectation(
            spark_session=spark_session,
            data=self.__result_expectation,
            execution_id=execution_id,
        )

        IngestionPatterns.write_file_gcs(
            value=json.dumps(self.__result.to_json_dict(), indent=4),
            path=join(
                "gs://",
                data_quality_bucket,
                dataset,
                table,
                executed_at.date().isoformat(),
                execution_id + ".json",
            ),
        )

        result_execution.write.format("bigquery").mode("append").option(
            "project", project_id
        ).option("parentProject", project_id).option(
            "temporaryGcsBucket", temp_bucket
        ).option(
            "intermediateFormat", "orc"
        ).save(
            "data_quality.cnt_tab_data_quality_execucao"
        )

        result_expectation.write.format("bigquery").mode("append").option(
            "project", project_id
        ).option("parentProject", project_id).option(
            "temporaryGcsBucket", temp_bucket
        ).option(
            "intermediateFormat", "orc"
        ).save(
            "data_quality.cnt_tab_data_quality_expectativas_execucao"
        )

        return execution_id

    # -------------------------------------------------------------
    def send_notifications(self, slack_token: str, slack_ids: List[str], message: str):
        client = WebClient(token=slack_token)
        for slack_id in set(slack_ids):
            client.chat_postMessage(channel=slack_id, text=message)
