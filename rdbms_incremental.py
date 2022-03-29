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
    'input_s3_path',
    'input_data_format',
    'output_database',
    'output_table',
    'output_path',

]


args = getResolvedOptions(sys.argv, params)
region = args['region']
input_s3_path = args['input_s3_path']
input_data_format = args['input_data_format']
output_database = args['output_database']
output_table = args['output_table']
output_path = args['output_path']

glue_context = GlueContext(SparkContext.getOrCreate())
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)


# Create DynamicFrame from Data Catalog
client = boto3.client('glue', region_name=region)

df = spark.read.format("input_data_format").option("header", "true").option("path",input_s3_path).load()
df = df.withColumn("audit_process_load_dt_tmstmp", lit(current_timestamp))
glue = boto3.client('glue')


# Begin Lake Formation transaction
tx_id = glue_context.start_transaction(read_only=False)

try:
    df.write.format("parquet").mode("overwrite").option("path",output_path).saveAsTable(output_database+"."+output_table)
    glue_context.commit_transaction(tx_id)
except Exception:
    glue_context.cancel_transaction(tx_id)
    raise

job.commit()
