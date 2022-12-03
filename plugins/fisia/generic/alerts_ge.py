import sys
from datetime import datetime

from airflow.models.variable import Variable

path_import = Variable.get("IMPORT_PATH")

sys.path.append(path_import)

import libs.connection.notify as nt
import libs.connection.query as qr

sql = """SELECT EXISTS(SELECT cod_produto FROM `cnto-data-lake.refined.cnt_fat_estoque` as estoque WHERE not EXISTS (SELECT cod_produto FROM `cnto-data-lake.refined.cnt_dim_produto_consolidado` as produto WHERE estoque.cod_produto = produto.cod_produto)) AS result"""

compare = qr.running_bq_result(sql=sql, project="cnto-data-lake")
if compare["result"][0] == True:
    slack_msg = """
            :alphabet-white-d::alphabet-white-a::alphabet-white-t::alphabet-white-a: - :alphabet-white-q::alphabet-white-u::alphabet-white-a::alphabet-white-l::alphabet-white-i::alphabet-white-t::alphabet-white-y: . 
            *Camada*: Refined 
            *Tabelas*:   cnt_fat_estoque - cnt_dim_produto_consolidado
            *Carga*: {datetime}  
            *Status*: Aleta de Sku com estoque sem cadastro de Produto.
            *Link* : Link com relatório -> https://datastudio.google.com/s/tjL_rjkBsWA 
            """.format(
        datetime.now()
    )
    nt.task_generic_slack_alert(slack_msg)
else:
    True
