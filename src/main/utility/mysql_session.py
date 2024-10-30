import mysql.connector

from resources.dev import config1
def get_mysql_connection():
    connection = mysql.connector.connect(
        host= config1.my_sql_host,
        user=config1.my_sql_user,
        password= config1.my_sql_password,
        database=config1.my_sql_database
    )
    return connection
