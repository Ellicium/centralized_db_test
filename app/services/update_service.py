import os
import math
import logging
import pandas as pd
from time import time
import datetime
from dotenv import load_dotenv
from fastapi.logger import logger

gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

load_dotenv()

def timer_func(func):
    # This function shows the execution time of
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func

def set_env_var():
    global sqlUserName,sqlPassword,sqlDatabaseName,sqlServerName,sqlSchemaName
    sqlUserName=os.getenv("sqlUserName")
    sqlPassword=os.getenv("sqlPassword")
    sqlDatabaseName=os.getenv("sqlDatabaseName")
    sqlServerName=os.getenv("sqlServerName")
    sqlSchemaName=os.getenv("sqlSchemaName")
    
def set_supplier_details_func(object,dbobj):
    try:
        set_env_var()
        
        level1Query = f'''SELECT * from {sqlSchemaName}.dim_supplier_info dsi 
                            where supplier_id = {object.Shipper_Id} and delete_flag is null'''
        level1Data = dbobj.read_table(level1Query)
        if(len(level1Data)):  
            insertNewData = f'''
                            Insert into {sqlSchemaName}.dim_supplier_info
                            (key_categories,supplier_additional_info,supplier_catalogue,supplier_capability,detailed_table,[source],key_customers,supplier_recognition,supplier_id,created_date,created_by)
                            SELECT key_categories,supplier_additional_info,supplier_catalogue,supplier_capability,detailed_table,[source],key_customers,supplier_recognition,supplier_id,created_date,created_by 
                            from {sqlSchemaName}.dim_supplier_info dsi 
                            where supplier_id = {object.Shipper_Id} and delete_flag is null '''
            updateDeleteData=f'''
                            update {sqlSchemaName}.dim_supplier_info
                            set delete_flag = 1
                            where supplier_id = {object.Shipper_Id} and id = {level1Data.iloc[0]['id']} '''
            updateInforQuery=f'''update {sqlSchemaName}.dim_supplier_info
                            set supplier_additional_info = '{object.Additional_Notes}',
                            supplier_capability = '{object.Capabilities}',
                            supplier_catalogue = '{object.Description}',
                            updated_date ='{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}',
                            updated_by = '{object.User}'
                            where supplier_id ={object.Shipper_Id} and delete_flag is null '''
            updateSupplierQuery=f'''update {sqlSchemaName}.dim_supplier
                            set supplier_type = '{object.Supplier_Type}',
                            ap_preferred = {object.Preferred_Supplier},
                            updated_date ='{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}',
                            updated_by = '{object.User}'
                            where id ={object.Shipper_Id} '''
            insertNewDataQueryResult=dbobj.execute_query(insertNewData)
            updateDeleteDataQueryResult=dbobj.execute_query(updateDeleteData)
            updateInforQueryResult=dbobj.execute_query(updateInforQuery)
            updateSupplierQueryQueryResult=dbobj.execute_query(updateSupplierQuery)
            return 'Data updated successfully'
        else:
            # updateInforQuery=f'''insert into  {sqlSchemaName}.dim_supplier_info
            #                 (supplier_additional_info,supplier_capability,supplier_id,created_by)
            #                 value
            #                 ('{object.Additional_Notes}','{object.Capabilities}',{object.shipper_id})
            #                  '''
            # updateInforQueryResult=dbobj.execute_query(updateInforQuery)
            return 'No Supplier Found'
    except Exception as e:
        logger.error("get_filters def failed",exc_info=e)