import os
#
from  src.main.utility.encrypt_decrypt import *
# [key_section]
key = "youtube_project"
iv = 'youtube_encyptyo'
salt = "youtube_AesEncryption"

# AWS bucket info
from src.main.utility.encrypt_decrypt import *
#
aws_access_key = "write access key here"
aws_secret_key = 'write secret key here'

bucket_name = 'project-bucket7'
s3_customer_datamart_directory ='customer_data_mart'
s3_sales_data_mart_directory = 'sales_data_mart'
s3_source_directory = 'sales_data/'
s3_processed_directory = 'sales_data_processed/'
s3_error_directory = 'sales_data_error/'
s3_sales_data_partitioned = 'sales_partitioned_data_mart'

# database detail
user = 'write user name here'
database = 'give database_name'
password = 'write password'
host = 'write host_name'

# table1 = 'product_staging_table'

# ====================== redshift python connection ================

rs_host = 'host_name extract from jdbc tha found in redshift name space '
rs_port = '5439'

rs_database = ' give database_name'
rs_user = 'write user name here'
rs_password = 'write password'

#Database credential
# MySQL database connection properties
my_sql_host = "localhost",
my_sql_user = 'write user name here',
my_sql_password = "write password",
my_sql_database = "give database_name"


url = f"jdbc:mysql://localhost:3306/{my_sql_database}"
properties = {
    "user": "write user name here",
    "password": "write password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

table1 = 'product_staging_table'

#====================  redshift credentials========================
redshift_url = "give JDBC url"
redshift_properties = {
    "user": "write user name here",
    "password": "write password",
    "driver": "com.amazon.redshift.jdbc42.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"


## mandatory schema

mandatory_schema = ["customer_id", 'store_id', 'product_name', 'sales_date', 'sales_person_id', 'price', 'quantity', 'total_cost']

# File Download location

local_directory = 'C:\\Users\\satya\\Documents\\project1\\file_from_s3\\'
customer_data_mart_local_file = 'C:\\Users\\satya\\Documents\\project1\\customer_data_mart\\'
sales_team_data_mart_local_file = 'C:\\Users\\satya\\Documents\\project1\\sales_team_data_mart\\'
sales_team_data_mart_partitioned_local_file = 'C:\\Users\\satya\\Documents\\project1\\sales_partition_data'
error_folder_path_local = 'C:\\Users\\satya\\Documents\\project1\\error_files\\'