import pandas as pd
import pyodbc
import os
import urllib.parse
from dotenv import load_dotenv
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from ..services.supplier_service import timer_func


load_dotenv()

def set_env_var():
    global sqlUserName,sqlPassword,sqlDatabaseName,sqlServerName,sqlSchemaName
    sqlUserName=os.getenv("sqlUserName")
    sqlPassword=os.getenv("sqlPassword")
    sqlDatabaseName=os.getenv("sqlDatabaseName")
    sqlServerName=os.getenv("sqlServerName")
    sqlSchemaName=os.getenv("sqlSchemaName")

set_env_var()

class database:
    @timer_func
    def __init__(self):

        driver="{ODBC Driver 17 for SQL Server}"
        #self.conn = pyodbc.connect(f'''DRIVER={driver}; Server={ServerName };
        #                        UID={SQLUser}; PWD={SQLPassword}; DataBase={DatabaseName}''')

        password = f"{sqlPassword}"
        params = urllib.parse.quote_plus(f"""Driver={driver};
                                        Server=tcp:{sqlServerName},1433;
                                        Database={sqlDatabaseName};
                                        Uid={sqlUserName};Pwd={password};
                                        Encrypt=yes;
                                        TrustServerCertificate=no;
                                        Connection Timeout=30;""")
        self.conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(
            params)
        self.engine = create_engine(self.conn_str, fast_executemany=True)

    # Fetching the data from the selected table using SQL query
    @timer_func
    def read_table(self,query):
        rawData= pd.read_sql_query(sql = text(query), con = self.engine.connect() )
        return rawData
    @timer_func
    def execute_query(self,query):
        #cursor=self.conn.cursor()
        #cursor.execute(query)
        #cursor.commit()
        connection = self.engine.connect()
        result = connection.execute(query)
        result.close()
        connection.close()
    @timer_func
    def insert_data(self, df:pd.DataFrame, table_name:str ,schema_name:str , chunksize = None, method=None):
        try :
            df.to_sql(table_name, con = self.engine ,if_exists='append', index=False , chunksize = chunksize,schema=schema_name, method=method)
        except Exception as e :
            print(e)
    # def close_conn_eng(self):
    #     try:
    #         self.conn.close()
    #     except Exception as e:
    #         print(e)