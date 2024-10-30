import os
import sys
from datetime import datetime


from os.path import curdir
import shutil

from pyspark.sql.functions import *
from pyspark.sql.types import *

from resources.dev import  config1
from src.main.delete.local_file_delete import delete_local_file
from src.main.read.database_read import  DatabaseReader
from src.main.transfer_files.move_error_files import move_file_to_s3
from src.main.transformations.jobs.customer_mart_sql_transform_write1 import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimensions_join_table
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import  upload_to_s3
from src.main.utility.mysql_session import get_mysql_connection
from src.main.utility.resdhift_python_connection import get_redshift_conn
from src.main.utility.s3_client_object import  *
from src.main.utility.logging_config import *
from src.main.utility.encrypt_decrypt import *
from src.main.utility.postgresql_session import *
from src.main.read.aws_read import *
from src.main.download.aws_file_download import *
from src.main.utility.spark_session import spark_session
from src.main.write.general_writer import GeneralWriter
from src.test.sales_data_csv import csv_file, sales_date
from src.main.write.redshift_writer import *



s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))

s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()

logger.info("list of bucket: %s", response['Buckets'])


# staging table
csv_files = []
for file in os.listdir(config1.local_directory):
      if file.endswith('.csv'):
          csv_files.append(file)

connection = get_redshift_conn()
cursor = connection.cursor()

# connection = get_mysql_connection()
# cursor = connection.cursor()
total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statment = f'''
              select distinct file_name from
              {config1.table1}
              where file_name in ({str(total_csv_files)[1:-1]}) and status = 'A'
            '''
    logger.info(f"dynamically statement created {statment}")

    cursor.execute(statment)
    data = cursor.fetchall()
    if data:
        logger.info('your last run is failed please check')
    else:
        logger.info('no record found')

else:
    logger.info('your last run was successful')




try:

    folder_path = config1.s3_source_directory

    s3_absolute_path = list_files(s3_client, config1.bucket_name, folder_path = folder_path)

    logger.info('Absolute path on s3 bucket for CSV files %s', s3_absolute_path)

    if not s3_absolute_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception('No data available to process')

except Exception as e:
     logger.error("Existed with error:- %s" ,e)


# 9:10 - 10:10
# 10:10 - project

prefix = f"s3://{config1.bucket_name}"  # bucket name without / so keep this thing in mind
file_paths = [url[len(prefix)+1:] for url in s3_absolute_path]

# 's3://project-bucket7/sales_data/line_delimited_json.json'
try:
    downloader = S3FileDownloader(s3_client, config1.bucket_name, config1.local_directory)

    downloader.download_files(file_paths)

except Exception as e:
    logger.info('file download error: %s', e)
    sys.exit()

# get a list of all file s in the local directory

all_files = os.listdir(config1.local_directory)

logger.info(f"List of files present at my local directory after download {all_files}")

if all_files:
    csv_files = []
    error_files = []

    for files in all_files:

        if files.endswith('.csv'):
            csv_files.append(os.path.abspath(os.path.join(config1.local_directory, files)))

        else:
            error_files.append(os.path.abspath(os.path.join(config1.local_directory,files)))

    if not csv_files:
       logging.info('No CSV data available to process the request')
       raise Exception('No CSV data available to process the request')


else:
    logging.info('There is no data to process')
    raise Exception("There is no data to process")

logger.info("*********************Listing the files********************")
logger.info("list of csv files that needs to be processed %s", csv_files)
logger.info("******************creating spark session *****************")

spark = spark_session()

logger.info('******************spark session created*******************')


# check the required column in the schema of csv files
# if required column is not present then sent to error_files
# else union all the data in one dataframe

logger.info('***********checking schema for data loaded in s3***********')

correct_files = []

for data in csv_files:
    data_schema = spark.read.format('CSV')\
                            .option('header', 'true')\
                            .load(data).columns # give all columns in file

    logger.info(f'Schema for  {data} is {data_schema}')
    logger.info(f'mandatory schema for data to process is {config1.mandatory_schema}')
    missing_columns = set(config1.mandatory_schema) - set(data_schema)
    logger.info(f"missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)

    else:
        logger.info(f'no missing columns for {data}')
        correct_files.append(data)

logger.info(f"***********list of correct files***********{correct_files}")
logger.info(f"***********list of incorrect files***********{error_files}")

logger.info(f"**********moving error data to error directory************")

# move data to error directory on local

error_folder_local_path = config1.error_folder_path_local

if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path,destination_path)

            logger.info(f"Moved'{file_name}' from s3 file path to {destination_path}")

            source_prefix = config1.s3_source_directory
            destination_prefix =config1.s3_error_directory

            message = move_file_to_s3(s3_client, config1.bucket_name, source_prefix, destination_prefix, file_name = None)
            logger.info(f"{message}")

        else:
            logger.info(f"'{file_path}' does not exist")
else:
    logger.info('*********There is no error files available in our dataset***********')


# what if any extra column come in file

# Before running the process

# stage table needs to updated with status as Active('A') 0r Inactive('I')


logger.info(f"******updating the product staging table that we have started the process******")

insert_statement = []

db_name = config1.database_name

current_date = datetime.now()

formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:

    for files in correct_files:
        file_name = os.path.basename(files)

        statements = f'''INSERT INTO {config1.table1}
                         (file_name, file_location, created_date, status)
                         values('{file_name}', '{file_name}', '{formatted_date}', 'A')
                      '''

        insert_statement.append(statements)
    logger.info(f"insert statement created for staging  --- {insert_statement}")
    logger.info('************ connecting with postgresql ***************')
    # connection = get_mysql_connection()
    connection = get_redshift_conn()
    cursor = connection.cursor()
    logger.info('************ postgresql connected successfully ***************')
    for statement in insert_statement:
        cursor.execute(statement)
        connection.commit()

    cursor.close()
    connection.close()

else:
    logger.info('*********** there is no files to process **************************')
    raise Exception('*************** No Data available with correct files ***********')



# final_df_to_process = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

logger.info(f"*******************staging table updated successfully*********************")

schema = StructType([
    StructField("customer_id", IntegerType(),True),
    StructField('store_id', IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField('quantity', IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

logger.info('********create empty dataframe ****************')

final_df_to_process = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

final_df_to_process.show()


for data in correct_files:

    data_df = spark.read.format('csv')\
                        .option('header', 'true')\
                        .option('inferSchema', 'true')\
                        .load(data)

    data_schema = data_df.columns

    extra_columns = list(set(data_schema) - set(config1.mandatory_schema))
    logger.info(f"extra column present in source data is {extra_columns}")

    if extra_columns:

        data_df = data_df.withColumn('additional_column', concat_ws(", ",*extra_columns))\
                         .select("customer_id", 'store_id', 'product_name', 'sales_date', 'sales_person_id', 'price', 'quantity', 'total_cost','additional_column')


        logger.info(f"processed {data} and added 'additional column'")

    else:
        data_df = data_df.withColumn('additional_column', lit(None)) \
                         .select("customer_id", 'store_id', 'product_name', 'sales_date', 'sales_person_id', 'price', 'quantity',
                            'total_cost', 'additional_column')


    final_df_to_process = final_df_to_process.union(data_df)

    final_df_to_process.show()


# logger.info(f"*****************final data from source going to processed****************************")
# final_df_to_process.show()
#
#
redshift_client = DatabaseReader(config1.redshift_url, config1.redshift_properties)
# mysql_client =  DatabaseReader(config1.url, config1.properties)
#
# # creating all tables
# #creating customer table
logger.info(f'********** loading customer table data into customer_table_df ***************')
customer_table_df = redshift_client.create_dataframe(spark, config1.customer_table_name)
# customer_table_df = mysql_client.create_dataframe(spark, config1.customer_table_name)

customer_table_df.show()
# product table
# logger.info(f'********** loading product table data into product_table_df ***************')
product_table_df = redshift_client.create_dataframe(spark, config1.product_table)
# product_table_df = mysql_client.create_dataframe(spark, config1.product_table)
#
# # product_staging_table table
logger.info(f'********** loading product_staging_table data into product_staging_table_df ***************')
product_staging_table_df = redshift_client.create_dataframe(spark, config1.product_staging_table)

# product_staging_table_df = mysql_client.create_dataframe(spark, config1.product_staging_table)
#
# # sales_team table
logger.info(f'********** loading sales_team table data into sales_team_table_df  ***************')
sales_team_table_df = redshift_client.create_dataframe(spark, config1.sales_team_table)

# sales_team_table_df = mysql_client.create_dataframe(spark, config1.sales_team_table)
# # store table
logger.info(f'********** loading store table table data into store_table_df  ***************')
store_table_df = redshift_client.create_dataframe(spark, config1.store_table)
#
# store_table_df = mysql_client.create_dataframe(spark,config1.store_table)
# # store_table_df.show()
s3_customer_store_sales_df_join = dimensions_join_table(final_df_to_process,customer_table_df,store_table_df, sales_team_table_df )


#Final Enrich Data
logger.info(f'***************************** Final Enrich Data ********************************')
s3_customer_store_sales_df_join.show()

# write the customer data into customer data mart in parquet format
# file will be written to local first
# move the raw data to s3 bucket for reporting tool
# write reporting data into redshift table also


# logger.info('**********write the data into customer data mart *****************')
#
final_customer_data_mart_df = s3_customer_store_sales_df_join.select('customer_id', 'st.first_name', 'st.last_name',
                                                                  'st.address', 'st.pincode', 'phone_number', 'sales_date', 'total_cost' )
#
#
parquet_writer = GeneralWriter("overwrite", 'parquet')

parquet_writer.dataframe_writer(final_customer_data_mart_df,  config1.customer_data_mart_local_file)
#
# move data into s3 customer data mart
#
logger.info(f"upload data into s3 customer_data mart")
# s3_upload = UploadToS3(s3_client)
s3_directory = config1.s3_customer_datamart_directory
#
message = upload_to_s3(s3_client, config1.customer_data_mart_local_file, config1.bucket_name, s3_directory)
logger.info(f'{message}')

# #sales_team_data mart
logger.info('**********write the data into sales team data mart *****************')
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
                                .select("store_id", 'sales_person_id', 'sales_person_first_name' ,
                                        'sales_person_last_name','store_manager_name', 'manager_id',
                                        'is_manager', 'sales_person_address', 'sales_person_pincode'
                                        ,'sales_date', 'total_cost',
                                        substring(col('sales_date'), 1,7).alias('sales_month'))
#
#
logger.info('**********final data for sales team datamart data mart *****************')
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config1.sales_team_data_mart_local_file)

# move data into s3 customer data mart

logger.info(f"upload data into s3 sales_team_data mart")
s3_sales_directory = config1.s3_sales_data_mart_directory
message = upload_to_s3(s3_client, config1.sales_team_data_mart_local_file, config1.bucket_name, s3_sales_directory)
logger.info(f'{message}')

#Also writing the data into partitions

final_sales_team_data_mart_df.write.format('parquet')\
                                    .option('header', 'true')\
                                   .mode('overwrite')\
                                   .partitionBy('sales_month', 'store_id')\
                                .option('path', config1.sales_team_data_mart_partitioned_local_file)\
                                .save()
logger.info(f'data is uploaded in sales_team_data_mart_partitioned_local_file')
# move data on s3 for partitioned folder
#
bucket_name = config1.bucket_name
current_epoch = int(datetime.now().timestamp()) * 1000
# s3_path = f"s3a://project-bucket7/sales_partitioned_data_mart/sales_data_{current_epoch}.parquet"
s3_path = f"s3a://{bucket_name}/{config1.s3_sales_data_partitioned}/sales_data_{current_epoch}.parquet"

final_sales_team_data_mart_df.write.format('parquet')\
                                    .option('header', 'true')\
                                    .mode("overwrite")\
                                    .partitionBy('sales_month' , 'store_id')\
                                    .option('path', s3_path)\
                                    .save()

logger.info(f'move data on s3 for partitioned folder successfully {s3_path}' )

# Calculation for customer mart
# Find out the customer total purchase every month
# write data into redshift


logger.info('*********calculating customer every month purchase amount*******************')
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info('*********data write successfully in customer_data_mart_table*******************')

logger.info('*********calculating sales_team incentive*******************')

sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info('*********data write successfully in sales_team_data_mart_table*******************')


# move data on s3  to processed folder and delete file from local path

source_prefix = config1.s3_source_directory
destination_prefix = config1.s3_processed_directory
message = move_file_to_s3(s3_client, config1.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")


logger.info('***********deleting sales data from local')
delete_local_file(config1.local_directory)
logger.info('***********deleted sales data from local')


logger.info('***********deleting customer_data_mart_local_file')
delete_local_file(config1.customer_data_mart_local_file)
logger.info('***********deleted customer_data_mart_local_file')


logger.info('***********deleting sales_team_data_mart_local_file')
delete_local_file(config1.sales_team_data_mart_local_file)
logger.info('***********deleted sales_team_data_mart_local_file')


logger.info('***********deleting sales_team_data_mart_partitioned_local_file')
delete_local_file(config1.sales_team_data_mart_partitioned_local_file)
logger.info('***********deleted sales_team_data_mart_partitioned_local_file')

# update status in staging table

update_statements = []
if correct_files:

    for files in correct_files:
        file_name = os.path.basename(files)

        statements = f"""update {config1.table1}
                      set status = 'I', updated_date ='{formatted_date}'
                      where file_name = '{file_name}'
                       """
        update_statements.append(statements)

    logger.info(f'update statement created for staging table {update_statements}')
    logger.info('***************** creating my_sql connection***************')
    # connection = get_mysql_connection()
    connection = get_redshift_conn()
    cursor = connection.cursor()

    logger.info('***************** my_sql connection created successfully ***************')

    for statement in update_statements:

        cursor.execute(statement)
        connection.commit()

    cursor.close()
    connection.close()

else:
    logger.info('there is some error in between')
    sys.exit()

input("press enter to terminate spark session")
