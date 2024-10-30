import traceback
from os import error

from src.main.utility.logging_config import logger


# from sys import prefix


def list_files(s3_client, bucket_name, folder_path):

     try:
         response = s3_client.list_objects_v2(Bucket = bucket_name, Prefix = folder_path)

         if 'Contents' in response:

             logger.info("Total files available in folder '%s' of bucket '%s':'%s'", folder_path , bucket_name, response)

             files = []
             for obj in response['Contents']:
                 if not obj['Key'].endswith('/'):
                     files.append(f"s3://{bucket_name}/{obj['Key']}")

             return files
         else:
             return []


     except Exception as e:
         error_message = f"error listing files:{e}"
         traceback_message = traceback.format_exc()
         logger.info("got this error : %s", error_message)
         raise



