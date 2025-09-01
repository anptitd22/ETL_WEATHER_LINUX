import os
import cx_Oracle
from dotenv import load_dotenv

#use env
path_env = "../.env"

load_dotenv(path_env)

SQL_TEST_HOST = os.getenv("SQL_TEST_HOST")
SQL_TEST_PORT = int(os.getenv("SQL_TEST_PORT"))
SQL_TEST_SERVICE = os.getenv("SQL_TEST_SERVICE")
SQL_TEST_USER = os.getenv("SQL_TEST_USER")
SQL_TEST_PASS = os.getenv("SQL_TEST_PASS")

def get_oracle_conn():
    # Sử dụng service
    dsn = cx_Oracle.makedsn(SQL_TEST_HOST, SQL_TEST_PORT, service_name=SQL_TEST_SERVICE)
    print(dsn)
    return cx_Oracle.connect(SQL_TEST_USER, SQL_TEST_PASS, dsn, encoding="UTF-8")

#other: use connections in airflow