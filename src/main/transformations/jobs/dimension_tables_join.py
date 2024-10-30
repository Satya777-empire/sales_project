
# import src.main.utility.spark_session
# from src.main.transformations.main import *
from src.main.utility.logging_config import *
from pyspark.sql.functions import *

def dimensions_join_table(final_df_to_process, customer_tables_df, store_table_df, sales_team_table_df):

        final_df_to_process = final_df_to_process.withColumnRenamed('customer_id', 'cust_id')
        df =  final_df_to_process.join(customer_tables_df,
                                              col('cust_id') == col('customer_id'),
                                              'inner')\
                                .drop('product_name' ,'price', 'quantity', 'additional_column', 'cust_id', 'customer_joining')
        logger.info("Joining the s3_customer_df_join with store_table_df ")
        # df.printSchema()

        s3_customer_store_df_join = df.join(store_table_df, df["store_id"] == store_table_df["id"], 'inner')\
                                .drop('id', 'store_pincode', 'store_opening_date', 'reviews')

        # s3_customer_store_df_join.show()
        # s3_customer_store_df_join.printSchema()

        logger.info("Joining the s3_customer_store_df_join with sales_team_table_df ")
        s3_customer_store_sales_df_join = s3_customer_store_df_join.join(sales_team_table_df.alias("st"),
                                                                         col("st.id") == s3_customer_store_df_join[
                                                                             "sales_person_id"],
                                                                         "inner") \
            .withColumn("sales_person_first_name", col("st.first_name")) \
            .withColumn("sales_person_last_name", col("st.last_name")) \
            .withColumn("sales_person_address", col("st.address")) \
            .withColumn("sales_person_pincode", col("st.pincode")) \
            .drop("id", "st.first_name", "st.last_name", "st.address", "st.pincode")


        # s3_customer_store_sales_df_join.show()
        # s3_customer_store_sales_df_join.printSchema()
        return s3_customer_store_sales_df_join

# dimensions_join_table(final_df_to_process, customer_table_df,store_table_df, sales_team_table_df)