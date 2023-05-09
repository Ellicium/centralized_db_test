import pandas as pd
import pyodbc,urllib,os,uvicorn,fastapi
from fastapi import FastAPI, Request, Form
from fastapi.logger import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine

app = FastAPI(debug=True)

load_dotenv()

user_name=None
password=None
Database_Name=None
Server_Name=None
schema_name=None


class database:
    def __init__(self , SQLUser , SQLPassword , DatabaseName , ServerName ):
        driver="{ODBC Driver 17 for SQL Server}"
        self.conn = pyodbc.connect(f'''DRIVER={driver}; Server={ServerName }; 
                                UID={SQLUser}; PWD={SQLPassword}; DataBase={DatabaseName}''')
    
        server = ServerName
        database = DatabaseName
        user = SQLUser
        password = f"{SQLPassword}"
        driver = "{ODBC Driver 17 for SQL Server}"
        params = urllib.parse.quote_plus(f"""Driver={driver};
                                        Server=tcp:{server},1433;
                                        Database={database};
                                        Uid={user};Pwd={password};
                                        Encrypt=yes;
                                        TrustServerCertificate=no;
                                        Connection Timeout=30;""")
        self.conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(
            params)
        
    # Fetching the data from the selected table using SQL query
    def read_table(self,query):
        RawData= pd.read_sql_query(query, self.conn)
        return RawData
    
    def execute_query(self,query):
        cursor=self.conn.cursor()
        cursor.execute(query)
        cursor.commit()
        
    def close_conn_eng(self):
        self.conn.close()

    def insert_data(self, df, table_name ,schema_name):
        try :
            self.engine1 = create_engine(self.conn_str, fast_executemany=True)
            df.to_sql(table_name, con = self.engine1 ,if_exists='append', index=False,schema=schema_name)
        except Exception as e :
            print(e)    
            




def set_env_var():
    global user_name,password,Database_Name,Server_Name,schema_name
    user_name=os.getenv("sqlUserName")
    password=os.getenv("sqlPassword")
    Database_Name=os.getenv("sqlDatabaseName")
    Server_Name=os.getenv("sqlServerName")
    schema_name=os.getenv("sqlSchemaName")

@app.get("/api/v1/supplier-market-analysis/get-filter")
async def supplier_market_analysis():
    try:
        set_env_var()
        new_dbobj=database(user_name , password , Database_Name , Server_Name)
        query = f'''SELECT DISTINCT Level_1 as label, Level_1 as value
        from {schema_name}.Vendors_Data vd WHERE Level_1 is not null '''

        query1 = f'''SELECT DISTINCT Level_2 as label, Level_2 as value
        from {schema_name}.Vendors_Data vd WHERE Level_2 is not null '''

        query2 = f'''SELECT DISTINCT Level_3 as label, Level_3 as value
        from {schema_name}.Vendors_Data vd WHERE Level_3 is not null'''


        new_df=new_dbobj.read_table(query)
        level_1=new_df.to_dict('records')

        new_df=new_dbobj.read_table(query1)
        level_2=new_df.to_dict('records')

        new_df=new_dbobj.read_table(query2)
        level_3=new_df.to_dict('records')


        main_dict={}
        main_dict["message"]= "Data Retrived successfully"

        main_dict["level_1"]=level_1
        main_dict["level_2"]=level_2
        main_dict["level_3"]=level_3
        new_dbobj.close_conn_eng
        
        return main_dict
        
    except:
        return None

