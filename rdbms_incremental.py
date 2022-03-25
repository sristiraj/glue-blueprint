# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import sys
import boto3
from pyspark.context import SparkContext
from pyspark.sql.functions import withColumn
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ResolveChoice, DropNullFields
from awsglue.utils import getResolvedOptions


# Configure required parameters
params = [
    'JOB_NAME',
    'region',
    'input_database_connection',
    'input_table',
    'input_table_inc_column',
    'input_table_inc_col_dtype',
    'output_database',
    'output_table',
    'output_path',

]


args = getResolvedOptions(sys.argv, params)
region = args['region']
input_database_connection = args['input_database_connection']
input_table = args['input_table']
output_database = args['output_database']
output_table = args['output_table']
output_path = args['output_path']
output_table_partition_col = args.get('output_table_partition_col',None)

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)


# Create DynamicFrame from Data Catalog
client = boto3.client('glue', region_name=region)
response = client.get_connection(Name=input_database_connection)
connection_properties = response['Connection']['ConnectionProperties']
URL = connection_properties['JDBC_CONNECTION_URL']
url_list = URL.split("/")
host = "{}".format(url_list[-2][:-5])
port = url_list[-2][-4:]
database = "{}".format(url_list[-1])
user = "{}".format(connection_properties['USERNAME'])
pwd = "{}".format(connection_properties['PASSWORD'])
df = spark.read.format("jdbc").option("url", URL).option("user", user).option("password", pwd).option("query", "select * from {} ".format(input_table)).load()
df = df.withColumn("audit_process_load_dt_tmstmp")
glue = boto3.client('glue')

if output_table_partition_col is None or output_table_partition_col not in df.columns:
    output_table_partition_col = "audit_process_load_dt_tmstmp"


# Begin Lake Formation transaction
tx_id = glue_context.start_transaction(read_only=False)

try:
    df.write.format("parquet").mode("overwrite").option("path",output_path).saveAsTable(output_database+"."+output_table)
    glue_context.commit_transaction(tx_id)
except Exception:
    glue_context.cancel_transaction(tx_id)
    raise

job.commit()