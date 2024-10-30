import os
# from resources.dev.config1 import *

from src.main.utility.encrypt_decrypt import decrypt, aws_access_key, aws_secret_key
from src.main.utility.s3_client_object import *

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))

s3_client = s3_client_provider.get_client()

local_file_dir = 'C:\\Users\\satya\\Documents\\sale_data_to_s3\\'

def upload_data(s3_directory, bucket_name,local_file_dir):
    s3_prefix = f"{s3_directory}"

    try:
        for root, dir, files in os.walk(local_file_dir):
            for file in files:
                print(file)
                local_file_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}{file}"
                s3_client.upload_file(local_file_path, bucket_name,s3_key)

    except Exception as e:
        raise e

s3_directory = "sales_data/"

bucket_name = 'project-bucket7'


upload_data(s3_directory, bucket_name, local_file_dir)
