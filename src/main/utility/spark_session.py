import findspark

from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import *

from src.main.utility.logging_config import logger
from src.main.utility.encrypt_decrypt import *

def spark_session():
    spark = SparkSession.builder.master('local[5]')\
                         .appName('Project1') \
        .config("spark.jars", "C:\\redshift-jdbc42-2.1.0.30\\redshift-jdbc42-2.1.0.30.jar") \
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-j-9.1.0.jar") \
        .config("spark.hadoop.fs.s3a.access.key", decrypt(aws_access_key)) \
    .config("spark.hadoop.fs.s3a.secret.key", decrypt(aws_secret_key)) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraJavaOptions", "-Daws.java.v1.disableDeprecationAnnouncement=true") \
        .config("spark.executor.extraJavaOptions", "-Daws.java.v1.disableDeprecationAnnouncement=true") \
        .getOrCreate()
    logger.info('spark_session %s',spark)
    return spark


