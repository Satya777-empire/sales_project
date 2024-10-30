import os
import traceback
from datetime import datetime

from src.main.utility.logging_config import logger


# class UploadToS3:
#
#     def __int__(self,s3_client):
#         self.s3_client = s3_client

def upload_to_s3(s3_client, local_path, s3_bucket, s3_directory):

        current_epoch= int(datetime.now().timestamp()) * 1000

        s3_prefix = f"{s3_directory}/{current_epoch}"

        try:

            for root, dirs,files in os.walk(local_path):

                for file in files:
                    local_file_path = os.path.join(root, file)

                    s3_key = f"{s3_prefix}/{file}"

                    s3_client.upload_file(local_file_path,s3_bucket,s3_key)

                return f"data is successfully uploaded in {s3_directory} data mart"

        except Exception as e:

            logger.error(f'error uploading file : {str(e)}')
            traceback_message = traceback.format_exc()
            print(traceback_message)

            raise e



