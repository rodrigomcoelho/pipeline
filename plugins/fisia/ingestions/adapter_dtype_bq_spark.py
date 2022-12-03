MAPPING_BQ_TO_SPARK = {
    "BOOL": "BOOLEAN",
    "INT64": "LONG",
    "FLOAT64": "DOUBLE",
    "DOUBLE": "DOUBLE",
    "NUMERIC": "DECIMAL(38,6)",
    "STRING": "STRING",
    "BYTES": "BINARY",
    "STRUCT": "STRUCT",
    "ARRAY": "ARRAY",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
}

# -------------------------------------------------------------
def adapter_bq_to_spark(bq_dtype: str) -> str:
    for _bq_dtype, _spark_dtype in MAPPING_BQ_TO_SPARK.items():
        if bq_dtype.casefold() == _bq_dtype.casefold():
            return _spark_dtype.upper()

    raise ValueError(f"Data type `{bq_dtype}` not found!")


# -------------------------------------------------------------
def adapter_spark_to_bq(spark_dtype: str) -> str:
    for _bq_dtype, _spark_dtype in MAPPING_BQ_TO_SPARK.items():
        if spark_dtype.casefold() == _spark_dtype.casefold():
            return _bq_dtype.upper()

    raise ValueError(f"Data type `{spark_dtype}` not found!")
