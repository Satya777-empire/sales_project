import traceback

from src.main.utility.logging_config import logger


class GeneralWriter:

    def __init__(self, mode, data_format):
        self.mode = mode,

        self.data_format = data_format


    def dataframe_writer(self, df: object, file_path: object) -> object:
        try:
            df.write.format(self.data_format) \
                .option("header", "true") \
                        .mode("overwrite") \
                        .option("path", file_path) \
                        .save()

        except Exception as e:
            logger.error(f"Error writing the data : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e

        # def __init__(self, mode, data_format):
        #     self.mode = mode
        #     self.data_format = data_format

        # def dataframe_writer(self, df, file_path):
        #     try:
        #         df.write.format(self.data_format) \
        #             .option("header", "true") \
        #             .mode(self.mode) \
        #             .option("path", file_path) \
        #             .save()