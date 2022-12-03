#!/usr/bin/python3
import json
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests

path_import = "C:\\Users\\88234\\Documents\\GitHub\\data-engineering\\"  # Variable.get('IMPORT_PATH')
sys.path.append(path_import)
import libs.connection.bucket as bkt
import libs.connection.notify as ntf


def geraToken(vemail, vpass):
    """Esta função bate na api checklist para buscar gerar o token de acesso necessario para as outras rotas de busca de dados"""

    ## User e senha para autenticação
    authen = {"email": vemail, "password": vpass}

    ## A autenticação retona um texto estilo json, ex:
    ## {
    ##  "data": {
    ##      "token": "eyJ0eXeiOiJ8V1QiLC9hbGciOiJIUzI1NiJ9.eyJpc3MiOiJyejJfYXV0ZW50",
    ##      "id": 1234,
    ##      "name": "Teste Checklist"
    ##  }
    ##}
    try:
        responseAuth = requests.post(
            "https://api-checklist.rz2.com.br/integration/login", data=authen
        )
    except Exception as e:
        print("Erro ao tentar autenticar: {}".format(e))
        exit()

    ## Sobe a string em json
    contResponseAuth = json.loads(responseAuth.content)

    ## Esse token vale por 24h
    vtoken = contResponseAuth["data"]["token"]
    return vtoken


def geraList(**kwargs):
    """Esta função bate na api checklist para buscar especificamente Avaliacoes - BI - Avaliações com Respostas Avaliativas"""

    ## Definicao de variavies de controle
    vurl = kwargs["url"]
    vtoken = kwargs["token"]

    if "dini" in kwargs.keys() and "dfim" in kwargs.keys():
        if kwargs["dini"] is not None and kwargs["dfim"] is not None:
            vdini = kwargs["dini"]
            vdfim = kwargs["dfim"]
            vperiod = "inicio={}&fim={}&".format(vdini, vdfim)
        else:
            vperiod = ""
            vdini = datetime.now().strftime("%Y-%m-%d")
    else:
        vperiod = ""
        vdini = datetime.now().strftime("%Y-%m-%d")

    vnextPage = 1
    vlimit = 5000
    lista = []
    qtdErro = 0
    listaPagErro = {}
    limitRemaining = 1

    while True:

        if int(limitRemaining) <= 0:
            time.sleep(90)

        if vnextPage == 1:
            listaPagErro[vdini] = []

        print("{}?{}page={}&limit={}".format(vurl, vperiod, vnextPage, vlimit))

        try:
            response = requests.get(
                "{}?{}page={}&limit={}".format(vurl, vperiod, vnextPage, vlimit),
                headers={"Authorization": "Bearer {}".format(vtoken)},
            )
        except Exception as e:
            print("Erro ao tentar buscar na API: {}".format(e))
            exit()

        ## Pegando iterações faltantes por minuto
        vheaders = response.headers
        limitRemaining = vheaders["X-RateLimit-Remaining"]

        ## Converte em lista
        try:
            listaTmp = list(json.loads(response.content)["data"]["items"])
            if vnextPage == 1:
                lastPage = json.loads(response.content)["data"]["lastPage"]
                totalItems = json.loads(response.content)["data"]["total"]
                print("Last page: {}\r\nTotal itens: {}".format(lastPage, totalItems))
        except Exception as e:
            print("Erro ao tentar gerar lista no while: {}".format(e))
            listaPagErro[vdini].append(vnextPage)
            qtdErro = qtdErro + 1

        lista.extend(listaTmp)

        vnextPage = vnextPage + 1
        if vnextPage > lastPage:
            break

    # Adicionando campo de particionamento
    listaNova = []
    for i in lista:
        i["partitionedto"] = vdini[0:7]
        listaNova.append(i)

    return {"lista": listaNova, "lista_erro": listaPagErro, "erro": qtdErro}


def geraDf(**kwargs):

    vuser = kwargs["user"]
    vpasswd = kwargs["passwd"]
    vurl = kwargs["vurl"]
    provider = kwargs["provider"]
    vbucket = kwargs["vbucket"]
    vpath = kwargs["vpath"]
    vmode = kwargs["vmode"]
    resultList_errors = {"lista_erro": {}, "erro": 0}

    try:
        ## Gera token
        vtoken = geraToken(vuser, vpasswd)
    except Exception as e:
        print("Algo deu errado: {}.".format(e))
        raise

    if "fromd" in kwargs.keys() and "tod" in kwargs.keys():
        fromd = kwargs["fromd"]
        tod = kwargs["tod"]
        ranged = (
            abs(
                (
                    datetime.strptime(tod, "%Y-%m-%d")
                    - datetime.strptime(fromd, "%Y-%m-%d")
                ).days
            )
            + 1
        )
        datelist = pd.date_range(fromd, periods=ranged).tolist()
        for i in datelist:
            i2 = i + timedelta(days=1)
            resultList = geraList(
                dini=i.strftime("%Y-%m-%d"),
                dfim=i2.strftime("%Y-%m-%d"),
                url=vurl,
                token=vtoken,
            )
            resultList_errors["lista_erro"].update(resultList["lista_erro"])
            resultList_errors["erro"] = resultList_errors["erro"] + resultList["erro"]

            vdf = pd.DataFrame(resultList["lista"])
            # -------- mandar pro AWS
            # print(vdf)
            print("Enviado AWS")
            bkt.save_parquet(
                vdf,
                provider,
                vbucket,
                "{}{}/{}/{}/".format(
                    vpath, i.strftime("%Y"), i.strftime("%m"), i.strftime("%d")
                ),
                vmode,
                filename_prefix="from{}".format(i.strftime("%Y%m%d_")),
            )
            # ------- mandar pro AWS

    else:
        resultList = geraList(url=vurl, token=vtoken)
        resultList_errors["lista_erro"].update(resultList["lista_erro"])
        resultList_errors["erro"] = resultList_errors["erro"] + resultList["erro"]

        vdf = pd.DataFrame(resultList["lista"])
        # -------- mandar pro AWS
        # print(vdf)
        print("Enviado AWS")
        bkt.save_parquet(
            vdf,
            provider,
            vbucket,
            vpath,
            vmode,
            filename_prefix="from{}".format(date.today().strftime("%Y%m%d_")),
        )
        # ------- mandar pro AWS

    if resultList_errors["erro"] == 0:
        slack_msg = """
                :cool-doge: Task Sucess.

                *CHECKLIST FACIL*
                *Api*: {}  
                *Tot. Erros: {}*
                *List. pag erros*: {} 
        """.format(
            vurl,
            resultList_errors["erro"],
            resultList_errors["lista_erro"],
        )
    else:
        slack_msg = """
                :feelsweirdman: Task Failed.

                *CHECKLIST FACIL*
                *Api*: {}  
                *Tot. Erros: {}*
                *List. pag erros*: {} 
        """.format(
            vurl,
            resultList_errors["erro"],
            resultList_errors["lista_erro"],
        )

        ntf.task_generic_slack_alert(slack_msg=slack_msg)

    print(
        "Total erros: {}\r\nDatas e paginas com erro: {}".format(
            resultList_errors["erro"], resultList_errors["lista_erro"]
        )
    )


if __name__ == "__main__":
    # geraDf(user='', passwd='', vurl='https://api-checklist.rz2.com.br/integration/countries', provider='aws', vbucket='s3://4insights-centauro-datafiles/', vpath='DATA_FILE/CHECKLIST_FACIL/AIRFLOW/COUNTRIES/', vmode='overwrite')
    geraDf(
        user="",
        passwd="",
        vurl="https://api-checklist.rz2.com.br/integration/v1/bi-with-results",
        fromd="2021-09-10",
        tod="2021-09-14",
        provider="aws",
        vbucket="s3://4insights-centauro-datafiles/",
        vpath="DATA_FILE/CHECKLIST_FACIL/AIRFLOW/CHECKLISTS_RESULTS/",
        vmode="overwrite",
    )

    # def par(start, n):
    #    if start%2 == 0:
    #        result = [start]
    #        i = 1
    #        while i < n:
    #            result.append(start+2)
    #            start=start+2
    #            i=i+1
    #    else:
    #        start = start+1
    #        result = [start]
    #        i = 1
    #        while i < n:
    #            result.append(start+2)
    #            start=start+2
    #            i=i+1
    #
    #    return result
    # print(par(9,5))


# **_______________**
# /home/nifi/data-engineering/src/nifi/centauro/checklist_facil/APIChecklistFacilCheckListResultados.py


def analiseGet(vget, vtoken):
    response = requests.get(vget, headers={"Authorization": "Bearer {}".format(vtoken)})

    print(json.loads(response.content))
    print(response.headers)
    print("total: {}".format(json.loads(response.content)["data"]["total"]))
    print("perPage: {}".format(json.loads(response.content)["data"]["perPage"]))
    print("currentPage: {}".format(json.loads(response.content)["data"]["currentPage"]))
    print("lastPage: {}".format(json.loads(response.content)["data"]["lastPage"]))
    print("nextPage: {}".format(json.loads(response.content)["data"]["nextPage"]))
    print("prevPage: {}".format(json.loads(response.content)["data"]["prevPage"]))
