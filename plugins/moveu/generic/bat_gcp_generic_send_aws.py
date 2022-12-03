import logging
import sys
from datetime import datetime
from datetime import timezone as tz

import pandas as pd
from pytz import timezone

sys.path.append("C:\\Users\\88234\\Documents\\GitHub\\data-engineering\\")
import libs.connection.bucket as bc
import libs.connection.query as qr


def dtime_sp_txt(**kwargs):
    data_e_hora_atuais = datetime.now()
    fuso_horario = timezone("America/Sao_Paulo")
    data_e_hora_sao_paulo = data_e_hora_atuais.astimezone(fuso_horario)
    data_e_hora_sao_paulo_em_texto = data_e_hora_sao_paulo.strftime("%d/%m/%Y %H:%M")
    data_e_hora_sao_paulo_arquivo = data_e_hora_sao_paulo.strftime("%Y%m%d_%H%M")
    data_e_hora_atuais = datetime.now(tz=tz.utc)
    data_e_hora_atuais.strftime("%d")
    data_e_hora_atuais.strftime("%m")
    data_e_hora_atuais.strftime("%Y")
    return data_e_hora_sao_paulo_arquivo


def send_aws(**kwargs):
    logging.info("Inicio {}".format(dtime_sp_txt()))

    ## Parametros para execução
    vquery = kwargs["vquery"]
    vfield_dt = kwargs["vfield_dt"]
    vtable = kwargs["vtable"].upper()
    vprovider = kwargs["vprovider"].lower()
    vbucket = kwargs["vbucket"]
    part_type = kwargs["part_type"].upper()
    vmode = kwargs["vmode"]

    logging.info("Query : {}".format(vquery))

    ## Executa as queries e retorna um dataframe do pandas
    df = qr.select_bq(sql=vquery)

    if vfield_dt is None:

        vpath = "DATA_FILE/DADOS_GCP/{}/".format(vtable)

        logging.info("Enviando para AWS . .  .    .")
        ## Pega um dataframe e converte em parquet e envia pro bucket
        bc.save_parquet(df, vprovider, vbucket, vpath, vmode)

    else:

        dtp = df[vfield_dt]

        # print(type(dtp.unique()))

        # for x in dtp.unique():
        # print(x.strftime('%Y%m%d'))
        # print(x.strftime('%Y%m'))
        # print(x.strftime('%Y'))

        # print(pd.to_datetime(df['part']).dt.strftime('%Y%m%d'))

        if part_type == "D":
            range_part = pd.to_datetime(dtp).dt.strftime("%Y%m%d").unique()
            vstrtime = "%Y%m%d"
        elif part_type == "M":
            range_part = pd.to_datetime(dtp).dt.strftime("%Y%m").unique()
            vstrtime = "%Y%m"
        elif part_type == "Y":
            range_part = pd.to_datetime(dtp).dt.strftime("%Y").unique()
            vstrtime = "%Y"
        else:
            print("[Erro] - Esse tipo [part_type] de particionamento nao e aceito!")
            exit()

        # exit()
        # filtered_df = df[df_mask]
        # print(filtered_df)

        # print(max)
        # print(max - min)
        # print((max - min).days)
        # print(max - (max - min))
        # print(max - (max - min) ==  max - (max - min))

        logging.info("Enviando para AWS . .  .    .")

        for vcomparator in range_part:
            print("part: {}".format(vcomparator))

            if part_type == "D":
                vcomparatorp = "{}/{}/{}".format(
                    vcomparator[0:4], vcomparator[4:6], vcomparator[6:8]
                )
            elif part_type == "M":
                vcomparatorp = "{}/{}".format(vcomparator[0:4], vcomparator[4:6])
            elif part_type == "Y":
                vcomparatorp = vcomparator

            df_mask = pd.to_datetime(df["part"]).dt.strftime(vstrtime) == vcomparator
            # filtered_df = df[df_mask]
            # print(filtered_df)

            vpath = "DATA_FILE/DADOS_GCP/{}/{}/{}_{}.parquet".format(
                vtable, vcomparatorp, vtable, vcomparator
            )

            ## Pega um dataframe e converte em parquet e envia pro bucket
            bc.save_parquet(df[df_mask], vprovider, vbucket, vpath, vmode)


def teste():

    sql = """
            SELECT *, DATE(payload.createdAt) as part 
            FROM `dev-data-platform-291914.Raw.cnt_kfk_6000_ctr_customer_updated` 
            /*WHERE DATE(payload.createdAt) >= current_date()-40*/
            ORDER BY part desc
          """
    field = """part"""

    send_aws(
        vquery=sql,
        vfield_dt=field,
        part_type="d",
        vtable="cnt_kfk_6000_ctr_customer_updated",
        vprovider="aws",
        vbucket="s3://4insights-centauro-datafiles/",
        vmode="append",
    )
    # send_aws(vquery=sql, vfield_dt=None, part_type=None, vtable='cnt_kfk_6000_ctr_customer_updated', vprovider='aws', vbucket='s3://4insights-centauro-datafiles/', vmode='append')


if __name__ == "__main__":
    teste()
