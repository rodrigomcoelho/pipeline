from io import BytesIO

import awswrangler as wr
import boto3 as b3
import pandas as pd
from airflow.models.variable import Variable
from google.cloud import storage


def save_file_gcp(content, bucket, path):
    file = BytesIO()  ## "cria" um arquivo em disco (HD IO) vazio
    file.write(content)  ## salva no arquivo
    file.seek(0)  ## indica a primeira linha do arquivo

    # Explicitly use service account credentials by specifying the private key https://cloud.google.com/docs/authentication/production
    # file..
    # storage_client = storage.Client.from_service_account_json('gcp_service_account.json')

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(path)
    blob.upload_from_file(file)


def save_parquet_gcp(df, bucket, path, mode, filename_prefix=""):
    parquet_file = BytesIO()  ## "cria" um arquivo em disco (HD IO) vazio
    df.to_parquet(parquet_file)  ## salva o data frame no arquivo
    parquet_file.seek(0)  ## indica a primeira linha do arquivo

    # Explicitly use service account credentials by specifying the private key https://cloud.google.com/docs/authentication/production
    # file..
    # storage_client = storage.Client.from_service_account_json('gcp_service_account.json')

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(path)
    blob.upload_from_file(parquet_file)


def save_parquet_aws(vdf, vbucket, vpath, vmode, filename_prefix=""):
    complete_path = vbucket + vpath
    session = b3.session.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
    )
    # session = b3.session.Session( aws_access_key_id='', aws_secret_access_key='')
    wr.s3.to_parquet(
        df=vdf,
        path=complete_path,
        dataset=True,
        boto3_session=session,
        mode=vmode,
        filename_prefix=filename_prefix,
    )


def save_parquet(vdf, provider, vbucket, vpath, vmode, filename_prefix=""):
    save_parquet_functions[provider](vdf, vbucket, vpath, vmode, filename_prefix)


def load_parquet_gcp(path):
    return pd.read_parquet(path)


def load_parquet_aws(path):
    return wr.s3.read_parquet(path, dataset=True)


def load_parquet(path, provider):
    return load_parquet_functions[provider](path)


save_parquet_functions = {"aws": save_parquet_aws, "gcp": save_parquet_gcp}

load_parquet_functions = {"aws": save_parquet_aws, "gcp": save_parquet_gcp}
