# from pyspark.pandas import read_table

# from src.main.transformations.jobs.main import spark
from src.main.utility.logging_config import logger

class DatabaseWriter:

    def __init__(self, url, propertes):

        self.url =url
        self.properties = propertes


    def write_dataframe(self,df, table_name):


        try:
            logger.info('Write data into table')

            df.write.jdbc(url = self.url,
                            table = table_name,
                            mode = 'append',
                            properties = self.properties)

            logger.info(f"data successfully written into {table_name}")

        except Exception as e:

              return {f"message: error occurred at{e}"}