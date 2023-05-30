import pandas as pd
import logging
from fastapi import FastAPI, Request, Form
from fastapi.logger import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pyodbc,urllib,os
from fastapi.logger import logger
import utils
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

app = FastAPI(debug=True)

load_dotenv()

sqlUserName=None
sqlPassword =None
sqlDatabaseName=None
sqlServerName=None
sqlSchemaName=None

def set_env_var():
    global sqlUserName,sqlPassword,sqlDatabaseName,sqlServerName,sqlSchemaName
    sqlUserName=os.getenv("sqlUserName")
    sqlPassword =os.getenv("sqlPassword")
    sqlDatabaseName=os.getenv("sqlDatabaseName")
    sqlServerName=os.getenv("sqlServerName")
    sqlSchemaName=os.getenv("sqlSchemaName")


@app.post("/v1/rfi-rfp/search-suppliers/get-suppliers-information")
async def search_suppliers_get_suppliers_information_api_fun(supplier :str,category:str,region:str,level_1:str,level_2:str,level_3:str,text:str,project_id: int = Form(...)):
    try:
        set_env_var()
        new_dbobj=utils.database(sqlUserName , sqlPassword , sqlDatabaseName , sqlServerName)
        return utils.search_suppliers_get_suppliers_information(new_dbobj,sqlSchemaName,supplier,category,region,level_1,level_2,level_3,text,project_id)
    except Exception as e:
        print(e)
        return None