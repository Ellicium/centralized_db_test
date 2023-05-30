import logging
import os
from fastapi import FastAPI, Request, Form, Response
from fastapi.logger import logger
from dotenv import load_dotenv
from fastapi.logger import logger

import schemas
import utils

from database import database

gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

app = FastAPI(debug=True)

load_dotenv()


#################################################################


def set_env_var():
    global sqlUserName,sqlPassword,sqlDatabaseName,sqlServerName,sqlSchemaName
    sqlUserName=os.getenv("sqlUserName")
    sqlPassword=os.getenv("sqlPassword")
    sqlDatabaseName=os.getenv("sqlDatabaseName")
    sqlServerName=os.getenv("sqlServerName")
    sqlSchemaName=os.getenv("sqlSchemaName")


def countrywise_supplier_count(freetext, level_1, level_2,level_3):
    try:
        set_env_var()
        dbobj=database(sqlUserName , sqlPassword , sqlDatabaseName , sqlServerName)
        apiquery = f"""SELECT
                count(query1.supplier_id) as 'supplier_count',  
                upper(query1.country) as 'country',
                upper(query1.iso2) 'country_code'
            FROM
                (SELECT
                    ds.name,
                    asm.supplier_id,
                    dc.country,
                    dc.iso2
                FROM
                    {sqlSchemaName}.dim_supplier ds
                    RIGHT JOIN {sqlSchemaName}.address_supplier_mapping asm ON ds.id = asm.supplier_id
                    LEFT JOIN {sqlSchemaName}.dim_address da ON asm.address_id = da.id
                    LEFT JOIN {sqlSchemaName}.dim_country dc ON da.country_id = dc.id) query1
            Inner JOIN
                (SELECT
                    csm.supplier_id,
                    dc1.name level_1,
                    dc2.name level_2,
                    dc3.name level_3
                FROM
                    {sqlSchemaName}.dim_supplier ds
                    RIGHT JOIN {sqlSchemaName}.category_supplier_mapping csm ON ds.id = csm.supplier_id
                    LEFT JOIN {sqlSchemaName}.dim_category_level dcl ON csm.category_level_id = dcl.id
                    LEFT JOIN {sqlSchemaName}.dim_category dc1 ON dcl.level_1_category_id = dc1.id
                    LEFT JOIN {sqlSchemaName}.dim_category dc2 ON dcl.level_2_category_id = dc2.id
                    LEFT JOIN {sqlSchemaName}.dim_category dc3 ON dcl.level_3_category_id = dc3.id) query2
            ON query1.supplier_id = query2.supplier_id"""
        #if freetext or level_1 or level_2 or level_3:
        if True:
            flag = True
            if level_1:
                if flag:
                    apiquery += " WHERE"
                    flag = False
                else:
                    apiquery += " AND"
                apiquery += f" query2.level_1 = '{level_1}'"
            if level_2:
                if flag:
                    apiquery += " WHERE"
                    flag = False
                else:
                    apiquery += " AND"
                apiquery += f" query2.level_2 = '{level_2}'"
            if level_3:
                if flag:
                    apiquery += " WHERE"
                    flag = False
                else:
                    apiquery += " AND"
                apiquery += f" query2.level_3 = '{level_3}'"
            if freetext:
                if flag:
                    apiquery += " WHERE"
                    flag = False
                else:
                    apiquery += " AND"
                apiquery += f"""  LOWER(query1.name) like '{freetext}'
                or LOWER(query2.Level_1) like '{freetext}'
                or LOWER(query2.Level_2) like '{freetext}'
                or LOWER(query2.Level_3) like '{freetext}'"""
            if flag:
                apiquery += " WHERE"
                flag = False
            else:
                apiquery += " AND"

        apiquery += " query1.country is not null and iso2 is not null"
        apiquery += " group by query1.country, query1.iso2 order by supplier_count desc;"
        print(apiquery)
        countrywiseSupplierCount = dbobj.read_table(apiquery)
        return countrywiseSupplierCount
    except Exception as e:
        logger.error("countrywise_supplier_count failed",exc_info=e)

@app.post("/Suppliers/get-supplier-count", response_model=schemas.SupplierCountResponse)
async def get_supplier_count(apipostschema:schemas.SupplierCountPost):
    try:
        logger.info(f"freetext:{apipostschema.text}, level_1:{apipostschema.level_1}, level_2:{apipostschema.level_2}, level_3:{apipostschema.level_3}")
        data = countrywise_supplier_count(apipostschema.text, apipostschema.level_1, apipostschema.level_2, apipostschema.level_3)
        returnData = data.to_dict(orient='records')
        return Response(content=returnData, media_type="application/json",status_code=200)
    except Exception as e:
        logger.error("get_supplier_count failed",exc_info=e)


def get_filters():
    try:
        set_env_var()
        dbobj=database(sqlUserName , sqlPassword , sqlDatabaseName , sqlServerName)
        level1Query = f'''select 
                        DISTINCT dc.name as 'label', dc.name as 'value' 
                        from {sqlSchemaName}.dim_category_level dcl  
                        left join {sqlSchemaName}.dim_category dc on dcl.level_1_category_id = dc.id'''
        level2Query = f'''select 
                    DISTINCT dc.name as 'label', dc.name as 'value' 
                    from {sqlSchemaName}.dim_category_level dcl  
                    left join {sqlSchemaName}.dim_category dc on dcl.level_2_category_id = dc.id'''
        level3Query = f'''select 
                    DISTINCT dc.name as 'label', dc.name as 'value' 
                    from {sqlSchemaName}.dim_category_level dcl  
                    left join {sqlSchemaName}.dim_category dc on dcl.level_3_category_id = dc.id'''
        level1Data = dbobj.read_table(level1Query)
        level2Data = dbobj.read_table(level2Query)
        level3Data = dbobj.read_table(level3Query)
        return level1Data, level2Data, level3Data
    except Exception as e:
        logger.error("get_filters def failed",exc_info=e)


@app.get("/Suppliers/get-filters",response_model=schemas.FilterResponse)
async def get_filters_api():
    try:
        logger.info("get-filters api started")
        level1filter, level2filter, level3filter = get_filters()
        apiResponse = {
                            "message": "Data Retrived successfully",
                            "level_1": level1filter.to_dict(orient='records'),
                            "level_2": level2filter.to_dict(orient='records'),
                            "level_3" : level3filter.to_dict(orient='records'),
                        }
        return apiResponse
    except Exception as e:
        logger.error("get_filters failed",exc_info=e)


@app.post("/Suppliers/get-suppliers-information")
async def search_suppliers_get_suppliers_information_api_fun(supplier :str,category:str,region:str,level_1:str,level_2:str,level_3:str,text:str):
    try:
        set_env_var()
        new_dbobj=database(sqlUserName , sqlPassword , sqlDatabaseName , sqlServerName)
        return utils.search_suppliers_get_suppliers_information(new_dbobj,sqlSchemaName,supplier,category,region,level_1,level_2,level_3,text)
    except Exception as e:
        print(e)
        return None
