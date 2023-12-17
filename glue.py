import sys
import boto3
import json
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkConf
from pyspark.sql import DataFrame, Row
from pyspark.sql import SparkSession
from awsglue import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,ArrayType,MapType,LongType
from pyspark.sql.functions import from_json,col,to_json,json_tuple


def get_secret():

    secret_name = "redshift"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

secret_dict = json.loads(get_secret())

params = [
    'JOB_NAME',
    'TempDir',
  	'kafka_broker',
    'topic',
  	'consumer_group',
  	'startingOffsets',
  	'checkpoint_interval',
  	'checkpoint_location',
  	'aws_region'
]
args = getResolvedOptions(sys.argv, params)
conf = SparkConf()

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job_name = args['JOB_NAME']
kafka_broker = args['kafka_broker']
topic = args['topic']
consumer_group = args['consumer_group']
startingOffsets = args['startingOffsets']
checkpoint_interval = args['checkpoint_interval']
checkpoint_location = args['checkpoint_location']
aws_region = args['aws_region']

# super or flatten
complex_convert = "super"

maxerror = 0
redshift_host = secret_dict['host']
redshift_port = secret_dict['port']
redshift_username = secret_dict['username']
redshift_password = secret_dict['password']
redshift_database = "dev"
redshift_schema = "ads"
redshift_table = "ads_test_table"
redshift_tmpdir = "s3://aws-glue-assets-us-east-2/temp/"
tempformat = "CSV"
redshift_iam_role = "arn:aws:iam::aws-account:role/redshiftetl"

reader = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("maxOffsetsPerTrigger", "1000000") \
    .option("kafka.consumer.commit.groupid", consumer_group) \
    .option("failOnDataLoss", "false")

if startingOffsets == "earliest" or startingOffsets == "latest":
    reader.option("startingOffsets", startingOffsets)
else:
    reader.option("startingTimestamp", startingOffsets)

kafka_data = reader.load()
df = kafka_data.selectExpr("CAST(value AS STRING)")


def process_batch(data_frame, batchId):
    dfc = data_frame.cache()
    logger.info(job_name + " - my_log - process batch id: " + str(batchId) + " record number: " + str(dfc.count()))
    logger.info(job_name + " - my_log - process batch id: " + str(batchId) + " record : " + str(dfc.show(5, truncate=False)))
    if not data_frame.rdd.isEmpty():
        # json_schema = spark.read.json(dfc.rdd.map(lambda p: str(p["value"]))).schema
        # 定义外层schema
        json_schema = StructType([StructField("id", StringType(), True),
                                  StructField("vehicleCode", StringType(), True),
                                  StructField("receiveTime", StringType(), True),
                                  StructField("body", StringType(), True)
                                  ])

        # 使用定义的schema解析，不存在的字段会设置为空
        sdf = dfc.select(from_json(col("value"), json_schema).alias("kdata")).select("kdata.*")
        logger.info(job_name + " - my_log - process batch id: " + str(batchId) + "sdf record : " + str(sdf.show()))
        # 使用json_tuple函数解析header，不存在的字段会设置为null
        sdf_with_header = sdf.select("id", "vehicleCode", "receiveTime","body",
                        json_tuple(col("body"), "vehicleType").alias('vehicleType')
                        )
        logger.info(job_name + " - my_log - process batch id: " + str(batchId) + "dfc record : " + str(sdf_with_header.show()))
        source_view_name = "kafka_source_table_view"
        sdf_with_header.createOrReplaceGlobalTempView(source_view_name)
        sdf = spark.sql(
            "select id,vehicleCode,receiveTime,body,vehicleType  from {view_name} where receivetime!=''".format(
                view_name="global_temp." + source_view_name))

        logger.info(job_name + " - my_log - source sdf schema: " + sdf._jdf.schema().treeString())
        logger.info(job_name + " - my_log - source sdf: " + sdf._jdf.showString(5, 20, False))
        if complex_convert == "super":
            csdf = to_super_df(spark, sdf)
        elif complex_convert == "flatten":
            csdf = flatten_json_df(sdf)
        else:
            csdf = to_super_df(spark,flatten_json_df(sdf))
        logger.info(job_name + " - my_log - convert csdf schema: " + csdf._jdf.schema().treeString())
        logger.info(job_name + " - my_log - convert csdf: " + csdf._jdf.showString(5, 20, False))

        if not csdf.rdd.isEmpty():
            csdf.write \
            .format("io.github.spark_redshift_community.spark.redshift") \
            .option("url", "jdbc:redshift://{0}:{1}/{2}".format(redshift_host, redshift_port, redshift_database)) \
            .option("dbtable", "{0}.{1}".format(redshift_schema,redshift_table)) \
            .option("user", redshift_username) \
            .option("password", redshift_password) \
            .option("tempdir", redshift_tmpdir) \
            .option("tempformat", tempformat) \
            .option("extracopyoptions", "TRUNCATECOLUMNS region '{0}' maxerror {1} dateformat 'auto' timeformat 'auto'".format(aws_region, maxerror)) \
            .option("aws_iam_role",redshift_iam_role).mode("append").save()
    dfc.unpersist()
    logger.info(job_name + " - my_log - finish batch id: " + str(batchId))


def to_super_df(spark: SparkSession, _df: DataFrame) -> DataFrame:
    col_list = []
    for field in _df.schema.fields:

        if field.dataType.typeName() in ["struct", "array", "map"]:
            col_list.append("to_json({col}) as aws_super_{col}".format(col=field.name))
        else:
            col_list.append(field.name)
    view_name = "aws_source_table"
    _df.createOrReplaceGlobalTempView(view_name)
    df_json_str = spark.sql("select {columns} from {view_name}".format(columns=",".join(col_list),view_name="global_temp."+view_name))

    fields = []
    for field in df_json_str.schema.fields:
        if "aws_super_" in field.name:
            sf = StructField(field.name.replace("aws_super_", ""), field.dataType, field.nullable,
                             metadata={"super": True, "redshift_type": "super"})
        else:
            sf = StructField(field.name, field.dataType, field.nullable)
        fields.append(sf)
    schema_with_super_metadata = StructType(fields)
    df_super = spark.createDataFrame(df_json_str.rdd, schema_with_super_metadata)
    return df_super


def flatten_json_df(_df: DataFrame) -> DataFrame:
    flattened_col_list = []
    def get_flattened_cols(df: DataFrame, struct_col: str = None) -> None:
        for col in df.columns:
            if df.schema[col].dataType.typeName() != 'struct':
                if struct_col is None:
                    flattened_col_list.append(f"{col} as {col.replace('.', '_')}")
                else:
                    t = struct_col + "." + col
                    flattened_col_list.append(f"{t} as {t.replace('.', '_')}")
            else:
                chained_col = struct_col + "." + col if struct_col is not None else col
                get_flattened_cols(df.select(col + ".*"), chained_col)

    get_flattened_cols(_df)
    return _df.selectExpr(flattened_col_list)


save_to_redshift = df \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime="{0} seconds".format(checkpoint_interval)) \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

save_to_redshift.awaitTermination()
