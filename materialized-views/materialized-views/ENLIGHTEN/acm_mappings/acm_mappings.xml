## test

import requests, json
url = (
    f"http://aws-tmp-creds-service."
    "ic-enphase-1."
    "svc.cluster.local.:8000/credentials"
)
response = requests.get(url)
tmp_creds = json.loads(response.text)
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", tmp_creds["aws_access_key_id"])
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", tmp_creds["aws_secret_access_key"])
spark._jsc.hadoopConfiguration().set("fs.s3a.session.token", tmp_creds["aws_session_token"])
spark._jsc.hadoopConfiguration().set("spark.hadoop.io.compression.codecs", "com.incorta.codecs.CSVGzipCodec")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "bucket.vpce-05e6dd29347f54d95-p38av9kr.s3.us-east-1.vpce.amazonaws.com") 
spark._jsc.hadoopConfiguration().set("fs.s3a.signing-algorithm", "S3SignerType")

from pyspark.sql.types import *
from pyspark.sql.functions import col, expr, date_trunc, weekofyear, to_date, max, split, col, substring, from_unixtime, substring, to_timestamp 
from pyspark.sql import SparkSession, DataFrame
from functools import reduce

my_bucket = "dms-cdc"
s3_path = f"s3a://data-reporting-airflow/rds-master-nightly-load/enlighten_production/acm_mappings/*"
df_csv1 = spark.read.format('csv').option("header", "true").option("quote", "\"").option("escape", "\"").option("multiline", "true").load(s3_path).select("*")

dfcolumns = ['id', 'acm_sn', 'pcu_sn', 'pvm_sn', 'pvm_pn', 'vendor', 'created_at', 'updated_at']
dfschema = StructType([StructField('id', LongType(), True), StructField('acm_sn', StringType(), True), StructField('pcu_sn', StringType(), True), StructField('pvm_sn', StringType(), True), StructField('pvm_pn', StringType(), True), StructField('vendor', StringType(), True), StructField('created_at', TimestampType(), True), StructField('updated_at', TimestampType(), True)])
df_csv1 = df_csv1.select(*[col(col_name).cast(dfschema[col_name].dataType) for col_name in dfcolumns])
save(df_csv1)
