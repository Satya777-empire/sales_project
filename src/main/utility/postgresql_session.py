import psycopg2
import resources.dev.config1
from resources.dev import config1


def get_postgre_connection():
    conn = psycopg2.connect(
        user = config1.user,
        database = config1.database,
        password= config1.password,
        host = config1.host
    )
    return conn
