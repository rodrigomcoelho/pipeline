#!/usr/bin/env python
import io
import logging as logger
import sys
from datetime import datetime
from datetime import timezone as tz

import avro.schema as avro_schema
from avro.io import BinaryDecoder, DatumReader
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from pytz import timezone


def dtime_sp_txt(**kwargs):
    data_e_hora_atuais = datetime.now()
    fuso_horario = timezone("America/Sao_Paulo")
    data_e_hora_sao_paulo = data_e_hora_atuais.astimezone(fuso_horario)
    data_e_hora_sao_paulo_em_texto = data_e_hora_sao_paulo.strftime("%d/%m/%Y %H:%M")
    data_e_hora_sao_paulo_arquivo = data_e_hora_sao_paulo.strftime("%Y%m%d_%H%M%S")
    data_e_hora_atuais = datetime.now(tz=tz.utc)
    data_e_hora_atuais.strftime("%d")
    data_e_hora_atuais.strftime("%m")
    data_e_hora_atuais.strftime("%Y")
    return data_e_hora_sao_paulo_arquivo


def decode(msg_value, reader):
    """Decode avro format with avro schema custom"""
    message_bytes = io.BytesIO(msg_value)
    message_bytes.seek(5)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict


def kafka_consumer(**kwargs):
    """Consumes data from the Kafka Topic
    Arguments: schema_str (string);
               table_id (string);
               topic (string);
               vprovider ('aws' ou 'gcp')
               vbucket (string) - (exemple: 's3://4insights-centauro-datafiles/')
               vpath (string)
               vmode (string) - (append (Default), overwrite, overwrite_partitions)"""

    sys.path.append(kwargs["path_import"])
    import libs.connection.bucket as bc
    import libs.connection.ccloud_lib as ccloud_lib
    import libs.connection.query as query

    schema_str = kwargs["schema_str"]
    config_file = kwargs["config_file"]
    topic = kwargs["topic"]
    table_id = kwargs["table_id"]
    kwargs["vprovider"]
    vbucket = kwargs["vbucket"]
    vpath = kwargs["vpath"]
    kwargs["vmode"]
    vgroup_id = kwargs["vgroup_id"]
    timeout = kwargs["timeout"]

    print("----------------------------------------------------------")
    print("Tópico: {}".format(topic))
    print("Tabela: {}".format(table_id))
    print("----------------------------------------------------------")

    config = ccloud_lib.read_ccloud_config(config_file)
    schema_registry = SchemaRegistryClient(
        {
            "url": config["schema.registry.url"],
            "basic.auth.user.info": config["basic.auth.user.info"],
        }
    )
    print(config["sasl.username"])

    if schema_str == "":
        schema_str = schema_registry.get_latest_version(
            topic + "-value"
        ).schema.schema_str  # get_schema(100052).schema_str

    # print(schema_str)

    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(config)
    consumer_conf["group.id"] = vgroup_id
    consumer_conf["auto.offset.reset"] = "earliest"

    c = DeserializingConsumer(consumer_conf)
    c.subscribe([topic])

    try:
        while True:
            try:
                message = c.poll(5)
                if message is None:
                    # print("no message received by consumer")
                    # logger.info("no message received by consumer")
                    continue
                elif message.error() is not None:
                    print("error from consumer - {}".format(message.error()))
                    # logger.error(message.error())
                else:
                    try:
                        schema = avro_schema.parse(schema_str)
                        reader = DatumReader(schema)

                        # print(message.value())
                        event_dict = [decode(message.value(), reader)]
                        rtn = query.table_insert_bq(table_id, event_dict)
                        if rtn[0] != "New rows have been added":
                            bc.save_file_gcp(
                                message.value(),
                                vbucket,
                                vpath + "ERROR/" + dtime_sp_txt(),
                            )
                            exit()
                        # exit()
                        # print(event_dict)
                        # df = pd.DataFrame(event_dict)
                        ## Pega um dataframe e converte em parquet e envia pro bucket
                        # bc.save_parquet(df, vprovider, vbucket, vpath + (event_dict[0]['metadata']['createdAt'])[0:4] + "/" + (event_dict[0]['metadata']['createdAt'])[5:7] + "/" + (event_dict[0]['metadata']['createdAt'])[8:10] , vmode)
                        # logger.info(event_dict)
                        # print(event_dict)

                    except KeyError as e:
                        print("Failed to unpack message - {}".format(e))
                        # logger.info(e)
            except KeyboardInterrupt as e:
                c.close()
                print("shutting down")
                logger.info(e)
                break
            except Exception as e:
                # Report malformed record, discard results, continue polling
                bc.save_file_gcp(
                    message.value(), vbucket, vpath + "ERROR/" + dtime_sp_txt()
                )
                print("Message deserialization failed! - {}".format(e))
                # logger.exception(e)
                exit()
                continue

            print(timeout)
            print(dtime_sp_txt())
            if timeout < dtime_sp_txt()[9:15]:
                c.close()
                print("shutting down by timeout")
                logger.info("shutting down by timeout")
                break

    finally:
        # Leave group and commit final offsets
        c.close()


def teste():
    path_import = "C:\\Users\\88234\\Documents\\GitHub\\data-engineering"  # sys.path.append('/home/airflow/gcs/dags/data_engineering/')
    config_file = "C:\\Users\\88234\\Documents\\confluent_dev.config"

    schema_str = ""
    topic = "gruposbf.customer.single_view.person_updated_v1"
    table_id = "dev-data-platform-291914.Raw.cnt_kfk_6000_ctr_customer_updated"
    vprovider = "gcp"
    vbucket = r"ctno-landing-zone"
    vpath = r"DATA_FILE/CUSTOMER_SINGLE_VIEW/CUSTOMER_UPDATED_V1/"
    vmode = "append"
    vgroup_id = "data_platform_dev"

    kafka_consumer(
        schema_str=schema_str,
        topic=topic,
        table_id=table_id,
        vprovider=vprovider,
        vbucket=vbucket,
        vpath=vpath,
        vmode=vmode,
        vgroup_id=vgroup_id,
        path_import=path_import,
        config_file=config_file,
    )


if __name__ == "__main__":
    teste()
