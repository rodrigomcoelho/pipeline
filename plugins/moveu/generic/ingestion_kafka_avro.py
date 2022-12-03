# %%
import json
import sys

import pyspark.sql.functions as F
from confluent_kafka.schema_registry import SchemaRegistryClient
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
from pyspark.sql.types import *

SCALA_VERSION = "2.12"
SPARK_VERSION = "3.1.2"

spark = (
    SparkSession.builder.appName("poc_stream")
    .config(
        "spark.jars.packages",
        f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION},org.apache.spark:spark-streaming-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION},org.apache.spark:spark-avro_2.12:3.1.2,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.18.1",
    )
    .getOrCreate()
)
# %%
schema_str = sys.argv[1]
EH_SASL = sys.argv[2]
table_id = sys.argv[3]
project_id = sys.argv[4]
bucket_id = sys.argv[5]
raw_dataset = sys.argv[6]
vmode = sys.argv[7]
vgroup_id = sys.argv[8]
topic = sys.argv[9]
fisia_project_id = sys.argv[10]
ctr_project_id = sys.argv[11]
fisia_bucket_id = sys.argv[12]
ctr_bucket_id = sys.argv[13]

insert_ctr = ctr_project_id + "." + raw_dataset + "." + "cnt_" + table_id
insert_fisia = fisia_project_id + "." + raw_dataset + "." + "fis_" + table_id
target = project_id + "." + raw_dataset + "." + table_id
path = "gs://" + bucket_id + "/DATA_FILE/KAFKA/" + "sbf_" + table_id
path_sbf = "gs://" + bucket_id + "/DATA_FILE/" + "sbf_" + table_id
fisia_path = "gs://" + fisia_bucket_id + "/DATA_FILE/" + "fis_" + table_id
cnto_path = "gs://" + ctr_bucket_id + "/DATA_FILE/" + "cnt_" + table_id
# %%
fromAvroOptions = {"mode": "PERMISSIVE"}
schema_registry = SchemaRegistryClient(
    {
        "url": "https://psrc-0xx5p.us-central1.gcp.confluent.cloud",
        "basic.auth.user.info": "JDYZGYBPG4IWQE6D:zPrX6z1fe7F2HOMLW1ltHJ9M9o82US6dSxqxJFgd70PpZTimLumMTx02JnKlsRjY",
    }
)
schema_str = schema_registry.get_latest_version(f"{topic}" + "-value").schema.schema_str
print(schema_str)
new_schema = json.loads(schema_str)
print(new_schema)
# %%
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("subscribe", f"{topic}")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("inferSchema", True)
    .option("header", True)
    .load()
    .select(F.substring(F.col("value"), 6, 100000).alias("value"))
)
df2 = df.select(from_avro(F.col("value"), schema_str, fromAvroOptions).alias("value"))
df2.printSchema()
# %%
df3 = df2.select(
    "value.metadata.eventName",
    "value.metadata.producerName",
    col("value.metadata.createdAt").alias("createdAtmetadata"),
    col("value.metadata.company").alias("companymetadata"),
    "value.payload.*",
)
df3.printSchema()
# %%
if "createdAt".upper() in (name.upper() for name in df3.columns):
    df3 = (
        df3.withColumn(
            "createdAt",
            df3.createdAt.cast(TimestampType()) + F.expr("INTERVAL 3 HOURS"),
        )
        .withColumn(
            "updatedAt",
            df3.updatedAt.cast(TimestampType()) + F.expr("INTERVAL 3 HOURS"),
        )
        .withColumn("createdAtmetadata", df3.createdAtmetadata.cast(DateType()))
    )
    print("Tem createdAt e updatedAt")
elif "performedAt".upper() in (
    name.upper() for name in df3.columns
) and "canceledAt".upper() in (name.upper() for name in df3.columns):
    df3 = (
        df3.withColumn(
            "performedAt",
            df3.performedAt.cast(TimestampType()) + F.expr("INTERVAL 3 HOURS"),
        )
        .withColumn(
            "canceledAt",
            df3.canceledAt.cast(TimestampType()) + F.expr("INTERVAL 3 HOURS"),
        )
        .withColumn("createdAtmetadata", df3.createdAtmetadata.cast(DateType()))
    )
    print("Tem performedAt e canceledAt")
elif "performedAt".upper() in (name.upper() for name in df3.columns):
    df3 = df3.withColumn(
        "performedAt",
        df3.performedAt.cast(TimestampType()) + F.expr("INTERVAL 3 HOURS"),
    ).withColumn("createdAtmetadata", df3.createdAtmetadata.cast(DateType()))
    print("Tem performedAt")
else:
    df3 = df3.withColumn("createdAtmetadata", df3.createdAtmetadata.cast(DateType()))
    print("Tem só createdAtmetadata")
# %%
query = (
    df3.repartition(1)
    .writeStream.format("parquet")
    .outputMode("append")
    .option("parentProject", project_id)
    .option("checkpointLocation", path + "/" + "sbf_" + table_id + "_checkpoint")
    .trigger(processingTime="600 seconds")
    .start(path + "/" + "sbf_" + table_id + "_file")
)
# %%
query.awaitTermination(60)
# %%
query.status
# %%
# query.stop()
# %%
# vpath = "gs://sbf-landing-zone/DATA_FILE/CUSTOMER_UPDATED_V1_SPARK/updated/part*"
vpath = path + "/" + "sbf_" + table_id + "_file/part*"
dfp = spark.read.format("parquet").option("header", "true").load(vpath)
dfp.printSchema()
# dfp = dfp.withColumn('createdAtmetadata',dfp.createdAtmetadata.cast(DateType()))
dfschema = dfp.printSchema()
dfp.createOrReplaceTempView("member")
member = spark.sql("SELECT * FROM member")


def write_gcs(df, mode, path, partition):
    # if partition.lower() in (partition.lower() for partition in df.columns):
    if "createdAtmetadata".lower() in (partition.lower() for partition in df.columns):
        df = df.withColumn(partition, F.to_date(partition).cast("date"))
    else:
        partition = "createdAtmetadata"
        df = df.withColumn(partition, F.current_date())
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.mode(mode).partitionBy(partition).parquet(path)
    return df


# %%
def create_table_external_hive_partitioned(table_id, path, project):
    original_table_id = table_id
    # Example file:
    # gs://cloud-samples-data/bigquery/hive-partitioning-samples/autolayout/dt=2020-11-15/file1.parquet
    uri = path + "*"
    # TODO(developer): Set source uri prefix.
    source_uri_prefix = path
    # [END bigquery_create_table_external_hivepartitioned]
    table_id = original_table_id
    # [START bigquery_create_table_external_hivepartitioned]
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client(project=project)
    # Configure the external data source.
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [uri]
    external_config.autodetect = True
    # Configure partitioning options.
    hive_partitioning_opts = bigquery.external_config.HivePartitioningOptions()
    # The layout of the files in here is compatible with the layout requirements for hive partitioning,
    # so we can add an optional Hive partitioning configuration to leverage the object paths for deriving
    # partitioning column information.
    # For more information on how partitions are extracted, see:
    # https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs
    # We have a "/dt=YYYY-MM-DD/" path component in our example files as documented above.
    # Autolayout will expose this as a column named "dt" of type DATE.
    hive_partitioning_opts.mode = "AUTO"
    hive_partitioning_opts.require_partition_filter = False
    hive_partitioning_opts.source_uri_prefix = source_uri_prefix
    external_config.hive_partitioning = hive_partitioning_opts
    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    # [END bigquery_create_table_external_hivepartitioned]
    return table


# %%
def check_table_exists(table_id, project):
    try:
        bigquery.Client(project=project).get_table(table_id)
        return True
    except NotFound:
        return False


# %%
fisia = spark.sql('SELECT * FROM member where companymetadata = "FISIA"')
# write_gcs(fisia, 'overwrite', fisia_path, 'createdAtmetadata')
print("DF FISIA CREATED")
cnto = spark.sql('SELECT * FROM member where companymetadata = "CENTAURO"')
# write_gcs(cnto, 'overwrite', cnto_path, 'createdAtmetadata')
print("DF CNTO CREATED")

# write no lake-sbf
write_gcs(member, "overwrite", path_sbf, "createdAtmetadata")

if cnto.rdd.isEmpty():
    print("DF Sem informação")
else:
    write_gcs(cnto, "overwrite", cnto_path, "createdAtmetadata")
    if check_table_exists(insert_ctr, ctr_project_id) is False:
        create_table_external_hive_partitioned(
            insert_ctr, path=cnto_path, project=ctr_project_id
        )
    else:
        print("Tabela já existe!")
    print("Não nulo")

if fisia.rdd.isEmpty():
    print("DF Sem informação")
else:
    write_gcs(fisia, "overwrite", fisia_path, "createdAtmetadata")
    if check_table_exists(insert_fisia, fisia_project_id) is False:
        create_table_external_hive_partitioned(
            insert_fisia, path=fisia_path, project=fisia_project_id
        )
    else:
        print("Tabela já existe!")
    print("Não nulo")
