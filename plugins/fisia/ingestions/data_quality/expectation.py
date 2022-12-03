from collections.abc import Iterable
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from pyspark.sql import Row, SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class Expectation:

    EXECUTION_SCHEMA = dict(
        cod_bu=dict(CASTING_TYPE=str, STRUCT_TYPE=StringType(), NULLABLE=False),
        cod_execucao=dict(CASTING_TYPE=str, STRUCT_TYPE=StringType(), NULLABLE=False),
        nom_tabela=dict(CASTING_TYPE=str, STRUCT_TYPE=StringType(), NULLABLE=False),
        dat_execucao=dict(
            CASTING_TYPE=datetime, STRUCT_TYPE=TimestampType(), NULLABLE=False
        ),
        cod_execucao_dag=dict(
            CASTING_TYPE=str, STRUCT_TYPE=StringType(), NULLABLE=False
        ),
        flg_sucesso=dict(CASTING_TYPE=bool, STRUCT_TYPE=BooleanType(), NULLABLE=False),
        num_expectativa_total=dict(
            CASTING_TYPE=int, STRUCT_TYPE=IntegerType(), NULLABLE=True
        ),
        qtd_expectativa_sucesso=dict(
            CASTING_TYPE=int, STRUCT_TYPE=IntegerType(), NULLABLE=True
        ),
        qtd_expectativa_falha=dict(
            CASTING_TYPE=int, STRUCT_TYPE=IntegerType(), NULLABLE=True
        ),
        des_estrutura_json=dict(
            CASTING_TYPE=str, STRUCT_TYPE=StringType(), NULLABLE=True
        ),
    )

    EXPECTATION_SCHEMA = dict(
        cod_bu=dict(CASTING_TYPE=str, STRUCT_TYPE=StringType(), NULLABLE=False),
        cod_execucao=dict(CASTING_TYPE=str, STRUCT_TYPE=StringType(), NULLABLE=False),
        cod_configuracao_expectativa=dict(
            CASTING_TYPE=str, STRUCT_TYPE=StringType(), NULLABLE=False
        ),
        des_lista_coluna_expectativa=dict(
            CASTING_TYPE=list, STRUCT_TYPE=ArrayType(StringType()), NULLABLE=True
        ),
        qtd_registro_expectativa=dict(
            CASTING_TYPE=int, STRUCT_TYPE=IntegerType(), NULLABLE=True
        ),
        qtd_registro_origem_expectativa=dict(
            CASTING_TYPE=int, STRUCT_TYPE=IntegerType(), NULLABLE=True
        ),
        qtd_registro_inesperado_expectativa=dict(
            CASTING_TYPE=int, STRUCT_TYPE=IntegerType(), NULLABLE=True
        ),
        qtd_registro_vazio_expectativa=dict(
            CASTING_TYPE=int, STRUCT_TYPE=IntegerType(), NULLABLE=True
        ),
        flg_sucesso=dict(CASTING_TYPE=bool, STRUCT_TYPE=BooleanType(), NULLABLE=False),
    )

    # -------------------------------------------------------------
    def __init__(
        self,
        expectation_config: str,
        expectation_name: str,
        expectation: Callable,
        output_columns: Optional[List[str]] = None,
        output_element_count: Optional[List[str]] = None,
        output_origin_count: Optional[List[str]] = None,
        output_unexpected_count: Optional[List[str]] = None,
        output_missing_count: Optional[List[str]] = None,
        output_is_success: Optional[List[str]] = None,
    ):

        self.expectation_config = expectation_config
        self.expectation_name = expectation_name
        self.expectation = expectation

        self.output_settings = dict(
            des_lista_coluna_expectativa=output_columns or list(),
            qtd_registro_expectativa=output_element_count or list(),
            qtd_registro_origem_expectativa=output_origin_count or list(),
            qtd_registro_inesperado_expectativa=output_unexpected_count or list(),
            qtd_registro_vazio_expectativa=output_missing_count or list(),
            flg_sucesso=output_is_success or list(),
        )

    # -------------------------------------------------------------
    def execute(self, meta: dict = None, columns: List[str] = None, **kwargs) -> None:
        for column in columns or [None]:
            self.expectation(
                column,
                **kwargs,
                meta={
                    "expectation_config": self.expectation_config,
                    "expectation_name": self.expectation_name,
                    **(meta or {}),
                }
            )

    # -------------------------------------------------------------
    def _adapter_expectation(self, json: dict) -> dict:
        return dict(
            cod_configuracao_expectativa=str(self.expectation_config),
            **{
                key: self._load_result(json, self.EXPECTATION_SCHEMA[key], value)
                for key, value in self.output_settings.items()
            }
        )

    # -------------------------------------------------------------
    @classmethod
    def adapter_execution(
        cls,
        json: dict,
        expectations: Dict[str, "Expectation"],
        is_success: Optional[List[str]] = ["success"],
        evaluated_expectations: Optional[List[str]] = [
            "statistics",
            "evaluated_expectations",
        ],
        successful_expectations: Optional[List[str]] = [
            "statistics",
            "successful_expectations",
        ],
        unsuccessful_expectations: Optional[List[str]] = [
            "statistics",
            "unsuccessful_expectations",
        ],
    ) -> Tuple[List[Dict], List[Dict]]:
        result_execution = dict(
            flg_sucesso=cls._load_result(
                json, cls.EXECUTION_SCHEMA["flg_sucesso"], is_success
            ),
            num_expectativa_total=cls._load_result(
                json,
                cls.EXECUTION_SCHEMA["num_expectativa_total"],
                evaluated_expectations,
            ),
            qtd_expectativa_sucesso=cls._load_result(
                json,
                cls.EXECUTION_SCHEMA["qtd_expectativa_sucesso"],
                successful_expectations,
            ),
            qtd_expectativa_falha=cls._load_result(
                json,
                cls.EXECUTION_SCHEMA["qtd_expectativa_falha"],
                unsuccessful_expectations,
            ),
        )

        result_expectations = list()
        for exp in json["results"]:
            result_expectations.append(
                expectations[cls.get_expectation_config(exp)]._adapter_expectation(exp)
            )

        return [result_execution], result_expectations

    # -------------------------------------------------------------
    @classmethod
    def _load_result(cls, json: dict, schema: dict, output_setting: List[str]) -> Any:
        result = cls.null_safe(json, *output_setting)
        cast = schema["CASTING_TYPE"]

        if isinstance(schema["STRUCT_TYPE"], ArrayType):
            if not isinstance(result, Iterable) or isinstance(result, str):
                return cast([result])

        return cast(result) if result is not None else None

    # -------------------------------------------------------------
    @classmethod
    def get_expectation_config(
        cls,
        result: dict,
        meta_path: List[str] = ["expectation_config", "meta", "expectation_config"],
    ) -> str:
        return str(cls.null_safe(result, *meta_path))

    # -------------------------------------------------------------
    @classmethod
    def df_execution(
        cls,
        spark_session: SparkSession,
        data: List[Dict],
        bu_code: str,
        execution_id: str,
        table: str,
        executed_at: datetime,
        run_id: str,
        json: str,
    ) -> DataFrame:
        return spark_session.createDataFrame(
            data=[
                Row(
                    cod_bu=bu_code,
                    cod_execucao=execution_id,
                    nom_tabela=table,
                    dat_execucao=executed_at,
                    cod_execucao_dag=run_id,
                    **row,
                    des_estrutura_json=json
                )
                for row in data
            ],
            schema=StructType(
                [
                    StructField(column, struct["STRUCT_TYPE"], struct["NULLABLE"])
                    for column, struct in cls.EXECUTION_SCHEMA.items()
                ]
            ),
        )

    # -------------------------------------------------------------
    @classmethod
    def df_expectation(
        cls,
        spark_session: SparkSession,
        data: List[Dict],
        bu_code: str,
        execution_id: str,
    ) -> DataFrame:
        return spark_session.createDataFrame(
            data=[
                Row(cod_bu=bu_code, cod_execucao=execution_id, **row) for row in data
            ],
            schema=StructType(
                [
                    StructField(column, struct["STRUCT_TYPE"], struct["NULLABLE"])
                    for column, struct in cls.EXPECTATION_SCHEMA.items()
                ]
            ),
        )

    # -------------------------------------------------------------
    @staticmethod
    def null_safe(dct: dict, *keys: str) -> Any:
        if not keys:
            return None

        for key in keys:
            try:
                dct = dct[key]
            except:
                return None

        return dct
