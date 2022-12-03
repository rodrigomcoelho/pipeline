import logging
import os

import awswrangler as wr
import boto3 as b3
import cx_Oracle
import pandas as pd
import pyodbc
import redshift_connector as rc
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from google.cloud import bigquery


def query_odbc(query, conn_string):
    conn = pyodbc.connect(conn_string)

    df = pd.read_sql(query, conn)
    conn.close()

    return df


def query_mssql_hook(vcon_id, vquery):

    mssqlhook: MsSqlHook = MsSqlHook(mssql_conn_id=vcon_id)
    df = mssqlhook.get_pandas_df(sql=vquery)

    return df


def query_sql_server(vhost, vport, vdatabase, vuid, vpwd, vquery):

    conn_params = (vhost, vport, vdatabase, vuid, vpwd)
    conn_string = (
        "Driver={SQL Server};Server=%s,%s;Database=%s;UID=%s;PWD=%s" % conn_params
    )

    print(conn_string)

    return query_odbc(vquery, conn_string)


def query_redshift(vhost, vport, vdatabase, vuid, vpwd, vquery):

    conn = rc.connect(
        host=vhost,
        port=vport,
        database=vdatabase,
        user=vuid,
        password=vpwd,
    )

    df = wr.redshift.read_sql_query(vquery, con=conn)
    conn.close()

    return df


def query_athena(vquery, vdatabase, vworkgroup, vregion, vaccess_aws, vsecret_aws):
    ## os.getenv('AWS_ATHENA_WORKGROUP')
    session = b3.Session(
        region_name=vregion,
        aws_access_key_id=vaccess_aws,
        aws_secret_access_key=vsecret_aws,
    )
    id = wr.athena.start_query_execution(
        vquery, database=vdatabase, workgroup=vworkgroup, boto3_session=session
    )
    df = wr.athena.wait_query(id, boto3_session=session)

    return df


def query_oracle_old(vhost, vport, vservice_name, vlib_dir, vuid, vpwd, vquery):

    try:
        ## Define caminho para cliente oracle (necessario ter instalado ODBC, JDBC e Client usei caminho padrão C:\oracle\)
        cx_Oracle.init_oracle_client(lib_dir=vlib_dir)
    except Exception as e:
        print(
            "\r\nWhoops!\r\nO client oracle já foi iniciado\r\nMas tudo bem vamos continuar\r\nerro:"
        )
        print(e)

    dsn_tns = cx_Oracle.makedsn(
        vhost, vport, service_name=vservice_name
    )  # caso precise, coloque um 'r' antes de qualquer parametro se tiver caracter especial
    connection = cx_Oracle.connect(
        user=vuid, password=vpwd, dsn=dsn_tns
    )  # cria a conexão uid+"/"+pwd+"@"+db

    df = pd.read_sql(vquery, con=connection)

    connection.close()

    return df


def query_oracle(vcon_id, vquery):

    oracle_hook: OracleHook = OracleHook(oracle_conn_id=vcon_id)
    df = oracle_hook.get_pandas_df(sql=vquery)

    return df


def create_external_table_part_time_bigquery(
    vtable_id, vuri, vwritedisposition, vfield, vexpiration_ms
):
    ## Criando tabela no bigquery
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = vtable_id

    writedisposition = vwritedisposition
    if writedisposition == "WRITE_EMPTY":
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_EMPTY,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=vfield,
                expiration_ms=vexpiration_ms,
            ),
        )
    elif writedisposition == "WRITE_APPEND":
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=vfield,
                expiration_ms=vexpiration_ms,
            ),
        )
    elif writedisposition == "WRITE_TRUNCATE":
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=vfield,
                expiration_ms=vexpiration_ms,
            ),
        )
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=vfield,
                expiration_ms=vexpiration_ms,
            ),
        )

    uri = vuri

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    return "Count tabela {} rows.".format(destination_table.num_rows)


def create_external_table_bigquery(
    vtable_id,
    vuri,
    vwritedisposition,
    vproject,
    vpartition="",
    vclustered="",
    vsource_format="",
    vtype="",
    vexpiration_ms="",
):
    ## Criando tabela no bigquery
    # Construct a BigQuery client object.

    project = vproject
    vpartition = vpartition
    clustered = vclustered
    client = bigquery.Client(project=project)
    # client.insert_rows()
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = vtable_id

    writedisposition = vwritedisposition
    job_config = bigquery.LoadJobConfig()
    if writedisposition == "WRITE_EMPTY":
        job_config.write_disposition = "WRITE_EMPTY"
    elif writedisposition == "WRITE_APPEND":
        job_config.write_disposition = "WRITE_APPEND"
    elif writedisposition == "WRITE_TRUNCATE":
        job_config.write_disposition = "WRITE_TRUNCATE"
    else:
        job_config.write_disposition = "WRITE_APPEND"

    if vsource_format == "PARQUET":
        job_config.source_format = "PARQUET"
    elif writedisposition == "CSV":
        job_config.source_format = "CSV"
    else:
        job_config.source_format = "PARQUET"

    if vpartition != None:
        logging.info("Partitioned to {}".format(vpartition))

        if vtype == "HOUR":
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.HOUR,
                field=vpartition,
                expiration_ms=vexpiration_ms,
            )
        elif vtype == "MONTH":
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field=vpartition,
                expiration_ms=vexpiration_ms,
            )
        elif vtype == "YEAR":
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.YEAR,
                field=vpartition,
                expiration_ms=vexpiration_ms,
            )
        else:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=vpartition,
                expiration_ms=vexpiration_ms,
            )

    if clustered != None:
        job_config.clustering_fields = vclustered

    uri = vuri

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config, project=project
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    return "Count tabela {} rows.".format(destination_table.num_rows)


def running_bq(**kwargs):

    sql = kwargs["sql"]
    project = kwargs["project"]

    ## Criando tabela no bigquery
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project)

    job = client.query(sql)  # API request.
    logging.info("api request")
    job.result()  # Waits for the query to finish.
    logging.info("job finish")
    return True


def running_bq_result(**kwargs):

    sql = kwargs["sql"]
    project = kwargs["project"]

    ## Criando tabela no bigquery
    # Construct a BigQuery client object.
    client = bigquery.Client(project=project)

    job = client.query(sql)  # API request.
    logging.info("api request")
    # Waits for the query to finish.
    result = job.result().to_dataframe()
    logging.info("job finish")
    return result


def save_dw(df, schema, table):
    host = os.getenv("AWS_REDSHIFT_HOST")
    database = os.getenv("AWS_REDSHIFT_DATABASE")
    username = os.getenv("AWS_REDSHIFT_SERVER_USERNAME")
    password = os.getenv("AWS_REDSHIFT_SERVER_PASSWORD")

    conn = rc.connect(user=username, database=database, password=password, host=host)

    wr.redshift.to_sql(df, conn, schema=schema, table=table, mode="overwrite")


def table_insert_bq(table_id, rows_to_insert):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of table to append to.
    # table_id = "your-project.your_dataset.your_table"

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        errors = ["New rows have been added"]
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    # [END bigquery_table_insert_rows]
    return errors


def drop_table_bq(dtable_id):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to fetch.
    # table_id = 'your-project.your_dataset.your_table'

    # If the table does not exist, delete_table raises
    # google.api_core.exceptions.NotFound unless not_found_ok is True.
    client.delete_table(dtable_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(dtable_id))


def create_table_bq(**kwargs):

    ctable_id = kwargs["table_id"]
    schema = eval("[" + kwargs["schema"] + "]")[0]
    pfield = kwargs["pfield"]
    ptype = kwargs["ptype"]
    exp_ms = kwargs["exp_ms"]

    if exp_ms == "":
        exp_ms = None
    elif exp_ms == 0:
        exp_ms = None
    elif exp_ms == "None":
        exp_ms = None

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to fetch.
    # table_id = 'your-project.your_dataset.your_table'

    table = bigquery.Table(ctable_id, schema=schema)

    if pfield == "" and ptype != "":
        if ptype == "day":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                expiration_ms=exp_ms,  # 90 days --> 7776000000
            )
        elif ptype == "month":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                expiration_ms=exp_ms,
            )
        elif ptype == "year":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.YEAR,
                expiration_ms=exp_ms,
            )
        elif ptype == "hour":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.HOUR,
                expiration_ms=exp_ms,
            )
        else:
            print("Error in definition of the partition part")

    elif pfield != "" and ptype != "":
        if ptype == "day":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=pfield,  # name of column to use for partitioning
                expiration_ms=exp_ms,
            )
        elif ptype == "month":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field=pfield,  # name of column to use for partitioning
                expiration_ms=exp_ms,
            )
        elif ptype == "year":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.YEAR,
                field=pfield,  # name of column to use for partitioning
                expiration_ms=exp_ms,
            )
        elif ptype == "hour":
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.HOUR,
                field=pfield,  # name of column to use for partitioning
                expiration_ms=exp_ms,
            )
        else:
            print("Error in definition of the partition part")

    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


def recreate_table_bq(**kwargs):

    kwargs = kwargs
    ctable_id = kwargs["table_id"]

    drop_table_bq(ctable_id)
    create_table_bq(**kwargs)


def get_schema_bq(table_id):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table
    #                  to add an empty column.
    # table_id = "your-project.your_dataset.your_table_name"

    table = client.get_table(table_id)  # Make an API request.

    original_schema = table.schema

    print("Get schema of table '{}'.".format(table_id))
    return original_schema


def print_kwargs(**kwargs):

    for x in kwargs:
        print(x + ": " + type(kwargs[x]))


def select_bq(**kwargs):

    sql = kwargs["sql"]

    ## Criando tabela no bigquery
    # Construct a BigQuery client object.
    client = bigquery.Client()

    job = client.query(sql)  # API request.

    result = job.result().to_dataframe()  # Waits for the query to finish.

    return result


def get_schema_dev_recreate_prod_bq(**kwargs):

    ctable_name = kwargs["table_name"]
    # kwargs['schema'] = eval(get_schema_bq(Variable.get('GCP_PROJECT_ID_DEV') + '.' + Variable.get('GCP_DATASET_RAW_DEV') + '.' + ctable_name))[0]
    kwargs["schema"] = get_schema_bq(
        "dev-data-platform-291914.Raw.{}".format(ctable_name)
    ).__str__()

    kwargs["pfield"]
    kwargs["ptype"]
    kwargs["exp_ms"]

    # kwargs['table_id'] = eval(get_schema_bq(Variable.get('GCP_PROJECT_ID') + '.' + Variable.get('GCP_DATASET_RAW') + '.' + ctable_name))[0]
    kwargs["table_id"] = "cnto-data-lake.raw." + ctable_name
    recreate_table_bq(**kwargs)


def teste():

    schema = """
[
SchemaField('metadata', 'RECORD', 'NULLABLE', None, (
				SchemaField('created_at', 'STRING', 'NULLABLE', None, (), None), 
				SchemaField('producer_name', 'STRING', 'NULLABLE', None, (), None), 
				SchemaField('table_name', 'STRING', 'NULLABLE', None, (), None)
), None), 
SchemaField('payload', 'RECORD', 'NULLABLE', None, (
				SchemaField('bu', 'STRING', 'NULLABLE', None, (), None), 
				SchemaField('department_id', 'INTEGER', 'NULLABLE', None, (), None), 
				SchemaField('dt', 'STRING', 'NULLABLE', None, (), None), 
				SchemaField('exclusion_type', 'STRING', 'NULLABLE', None, (), None), 
				SchemaField('id', 'INTEGER', 'NULLABLE', None, (), None), 
				SchemaField('modified_at', 'STRING', 'NULLABLE', None, (), None), 
				SchemaField('store_id', 'STRING', 'NULLABLE', None, (), None)
), None)
]
            """

    # dtable_id = Variable.get('GCP_PROJECT_ID') + '.' + Variable.get('GCP_DATASET_RAW') + '.' + "cnt_kfk_6000_ctr_person_updated"
    # ctable_id = Variable.get('GCP_PROJECT_ID') + '.' + Variable.get('GCP_DATASET_RAW') + '.' + "cnt_kfk_6000_ctr_person_updated"
    # table_id = "dev-data-platform-291914.Raw.cnt_kfk_6000_all_exclusion_group"
    # table_name = "cnt_kfk_6000_all_exclusion_group"
    # print(type(eval('[' + schema + ']')[0]))
    # print(type(get_schema_bq(table_id)))
    # print((eval('[' + schema + ']')[0]))
    # print((get_schema_bq(table_id)))
    # drop_table_bq (ctable_id)
    # recreate_table_bq(table_id=table_id, schema=schema, pfield='', ptype='day', exp_ms='')
    # get_schema_dev_recreate_prod_bq(table_name=table_name, pfield='', ptype='day', exp_ms=None)
    # print(select_bq(sql='select * from dev-data-platform-291914.Raw.cnt_kfk_6000_ctr_customer_updated limit 10').head())
    print(
        query_athena(
            'ALTER TABLE centauro_spectrum_db.cnt_kfk_6000_all_simple_stock_policy ADD PARTITION (`_partitiondate` = "2021-10-18") LOCATION "s3://4insights-centauro-datafiles/DATA_FILE/cnt_kfk_6000_all_simple_stock_policy/_PARTITIONDATE=2021-10-18/"',
            "centauro_spectrum_db",
            "w_bi_dev",
            "us-east-1",
            "",
            "/++",
        )
    )


if __name__ == "__main__":
    teste()
