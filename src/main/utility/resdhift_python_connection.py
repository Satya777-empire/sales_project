import psycopg2

from resources.dev import config1






def get_redshift_conn():
        connection = psycopg2.connect(
                        host = config1.rs_host,
                        port = config1.rs_port,
                        dbname = config1.rs_database,
                        user=  config1.rs_user,
                         password = config1.password)
        # print(connection)
        return connection





