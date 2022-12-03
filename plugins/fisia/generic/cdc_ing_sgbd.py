import logging
import uuid
from datetime import datetime
from datetime import timezone as tz

import libs.connection.bucket as bc
import libs.connection.query as qr
from pytz import timezone


def dtime_sp_txt(**kwargs):
    data_e_hora_atuais = datetime.now()
    fuso_horario = timezone("America/Sao_Paulo")
    data_e_hora_sao_paulo = data_e_hora_atuais.astimezone(fuso_horario)
    data_e_hora_sao_paulo_em_texto = data_e_hora_sao_paulo.strftime("%d/%m/%Y %H:%M")
    data_e_hora_sao_paulo.strftime("%Y%m%d_%H%M")
    data_e_hora_atuais = datetime.now(tz=tz.utc)
    data_e_hora_atuais.strftime("%d")
    data_e_hora_atuais.strftime("%m")
    data_e_hora_atuais.strftime("%Y")
    return data_e_hora_sao_paulo_em_texto


def oracle(**kwargs):
    logging.info("Inicio {}".format(dtime_sp_txt()))
    ## Parametros para execução
    vcon_id = kwargs["vcon_id"]
    vtable = kwargs["vtable"]
    logging.info("Obtendo dados : {}".format(vtable))
    vquery = kwargs["vquery"]
    vprovider = kwargs["vprovider"]
    vbucket = kwargs["vbucket"]
    vproject = kwargs["vproject"]
    vpath = r"DATA_FILE/" + vtable + "/" + vtable + "_" + uuid.uuid4().hex + ".parquet"
    vmode = kwargs["vmode"]
    kwargs["vschema_db"]
    vdataset = kwargs["vdataset"]
    vtable_id = vdataset + "." + vtable
    vuri = "https://storage.cloud.google.com/" + vbucket + "/" + vpath
    vwritedisposition = kwargs["vwritedisposition"]
    vpartition = kwargs["vpartition"]
    vclustered = kwargs["vclustered"]
    vsource_format = kwargs["vsource_format"]
    vtype = kwargs["vtype"]
    vexpiration_ms = kwargs["vexpiration_ms"]
    logging.info("Query : {}".format(vquery))
    ## Executa as queries e retorna um dataframe do pandas
    df = qr.query_oracle(vcon_id, vquery)
    logging.info("Enviando para GCP")
    ## Pega um dataframe e converte em parquet e envia pro bucket
    bc.save_parquet(df, vprovider, vbucket, vpath, vmode)
    logging.info("Carga realizada no bucket : {}".format(vbucket))
    ## WRITE_EMPTY	(Grava dados apenas se a tabela estiver vazia)
    ## WRITE_APPEND ((Padrão) Anexa os dados ao final da tabela)
    ## WRITE_TRUNCATE (Apaga todos os dados da tabela antes de gravar os novos. Essa ação também exclui o esquema da tabela e remove qualquer chave do Cloud KMS.)
    ## Cria tabela ou carrega novos dados
    qr.create_external_table_bigquery(
        vtable_id=vtable_id,
        vuri=vuri,
        vwritedisposition=vwritedisposition,
        vproject=vproject,
        vpartition=vpartition,
        vclustered=vclustered,
        vsource_format=vsource_format,
        vtype=vtype,
        vexpiration_ms=vexpiration_ms,
    )
    logging.info("Fim {}".format(dtime_sp_txt()))


def mssql(**kwargs):
    logging.info("Inicio {}".format(dtime_sp_txt()))
    ## Parametros para execução
    vcon_id = kwargs["vcon_id"]
    vtable = kwargs["vtable"]
    logging.info("Obtendo dados : {}".format(vtable))
    vquery = kwargs["vquery"]
    vprovider = kwargs["vprovider"]
    vbucket = kwargs["vbucket"]
    vpath = r"DATA_FILE/" + vtable + "/" + vtable + "_" + uuid.uuid4().hex + ".parquet"
    vmode = kwargs["vmode"]
    kwargs["vschema_db"]
    vproject = kwargs["vproject"]
    vdataset = kwargs["vdataset"]
    vtable_id = vdataset + "." + vtable
    vuri = "https://storage.cloud.google.com/" + vbucket + "/" + vpath
    vwritedisposition = kwargs["vwritedisposition"]
    vpartition = kwargs["vpartition"]
    vclustered = kwargs["vclustered"]
    vsource_format = kwargs["vsource_format"]
    vtype = kwargs["vtype"]
    vexpiration_ms = kwargs["vexpiration_ms"]
    logging.info("Query : {}".format(vquery))
    ## Executa as queries e retorna um dataframe do pandas
    df = qr.query_mssql_hook(vcon_id, vquery)
    logging.info("Enviando para GCP")
    ## Pega um dataframe e converte em parquet e envia pro bucket
    bc.save_parquet(df, vprovider, vbucket, vpath, vmode)
    ## WRITE_EMPTY	(Grava dados apenas se a tabela estiver vazia)
    ## WRITE_APPEND ((Padrão) Anexa os dados ao final da tabela)
    ## WRITE_TRUNCATE (Apaga todos os dados da tabela antes de gravar os novos. Essa ação também exclui o esquema da tabela e remove qualquer chave do Cloud KMS.)
    ## Cria tabela ou carrega novos dados
    qr.create_external_table_bigquery(
        vtable_id=vtable_id,
        vuri=vuri,
        vwritedisposition=vwritedisposition,
        vproject=vproject,
        vpartition=vpartition,
        vclustered=vclustered,
        vsource_format=vsource_format,
        vtype=vtype,
        vexpiration_ms=vexpiration_ms,
    )
    logging.info("Fim {}".format(dtime_sp_txt()))
