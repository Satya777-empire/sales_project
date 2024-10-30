from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from resources.dev import config1
from src.main.utility.logging_config import logger
from src.main.write.redshift_writer import DatabaseWriter



# url = f"jdbc:mysql://localhost:3306/{database_name}"
# properties = {
#     "user": "root",
#     "password": "Anus!0305",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }


def sales_mart_calculation_table_write(final_sales_team_data_mart_df):

    window = Window.partitionBy('store_id', 'sales_person_id', 'sales_month')

    sales_team_data_mart_df = final_sales_team_data_mart_df.withColumn( 'sales_month',
                                                                           substring('sales_date', 1,7))\
                                               .withColumn('total_sales_every_month', sum('total_cost').over(window))\
                                                .select('store_id', 'sales_person_id',
                                                        concat('sales_person_first_name', lit(' '), 'sales_person_last_name').alias('full_name'),
                                                        'sales_month', 'total_sales_every_month')
    w2 = Window.partitionBy(col('store_id'), col('sales_month')).orderBy(col('total_sales_every_month').desc())

    final_sales_team_data_mart = sales_team_data_mart_df.withColumn( 'rnk', rank().over(w2))\
                                                        .withColumn('incentive' , when(col('rnk') == 1, col('total_sales_every_month')* .01 )
                                                                                  .otherwise(lit(0)))\
                                                        .withColumn('incentive', round(col('incentive'),2))\
                                                        .withColumnRenamed('total_sales_every_month' ,'total_sales')\
                                                        .select('store_id', 'sales_person_id', 'full_name', 'sales_month', 'total_sales', 'incentive' )


    logger.info('write data into sales_team_data_mart')

    db_writer = DatabaseWriter(config1.redshift_url, config1.redshift_properties)
    db_writer.write_dataframe(final_sales_team_data_mart, config1.sales_team_data_mart_table )