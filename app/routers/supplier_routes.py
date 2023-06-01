import os
import logging
from fastapi import FastAPI, Request, Form, Response, APIRouter
from fastapi.logger import logger
from dotenv import load_dotenv
from ..config.db_config import database
from ..schemas.supplier_schema import SupplierCountPost, SupplierCountResponse,FilterResponse,SupplierInfo ,SupplierInfoResponse,SupplierCategoryWise,SupplierDetails
from ..services.supplier_service import countrywise_supplier_count, get_categorywise_count, return_null_if_none_category,get_filters, search_suppliers_get_suppliers_information,supplier_details_api

gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)
load_dotenv()

def set_env_var():
    global sqlUserName,sqlPassword,sqlDatabaseName,sqlServerName,sqlSchemaName
    sqlUserName=os.getenv("sqlUserName")
    sqlPassword=os.getenv("sqlPassword")
    sqlDatabaseName=os.getenv("sqlDatabaseName")
    sqlServerName=os.getenv("sqlServerName")
    sqlSchemaName=os.getenv("sqlSchemaName")

router = APIRouter()

set_env_var()

dbobj=database()

@router.post("/suppliers/get-supplier-count", response_model=SupplierCountResponse)
async def get_supplier_count(apipostschema:SupplierCountPost):
    try:
        logger.info(f"freetext:{apipostschema.text}, level_1:{apipostschema.level_1}, level_2:{apipostschema.level_2}, level_3:{apipostschema.level_3}")
        data = countrywise_supplier_count(apipostschema.text, apipostschema.level_1, apipostschema.level_2, apipostschema.level_3, dbobj)
        returnData = data.to_json(orient='records')
        return Response(content=returnData, media_type="application/json",status_code=200)
    except Exception as e:
        logger.error("get_supplier_count failed",exc_info=e)




@router.get("/suppliers/get-filters",response_model=FilterResponse)
async def get_filters_api():
    try:
        logger.info("get-filters api started")
        level1filter, level2filter, level3filter = get_filters(dbobj)
        apiResponse = {
                            "message": "Data Retrived successfully",
                            "level_1": level1filter.to_dict(orient='records'),
                            "level_2": level2filter.to_dict(orient='records'),
                            "level_3" : level3filter.to_dict(orient='records'),
                        }
        return apiResponse
    except Exception as e:
        logger.error("get_filters failed",exc_info=e)


@router.post("/suppliers/get-suppliers-information")
async def search_suppliers_get_suppliers_information_api_fun(apipostschema:SupplierInfo):
    try:
        set_env_var()
        new_dbobj=database()
        return search_suppliers_get_suppliers_information(new_dbobj,sqlSchemaName,apipostschema.supplier,apipostschema.category,apipostschema.region,apipostschema.level_1,apipostschema.level_2,apipostschema.level_3,apipostschema.text)
    except Exception as e:
        print(e)
        return None



@router.post("/suppliers/get-supplier-catogarywise")
async def search_suppliers_get_supplier_catogarywise_api_fun(apipostschema:SupplierCategoryWise):
    try:
        set_env_var()
        new_dbobj=database()
        return get_categorywise_count(new_dbobj,sqlSchemaName,apipostschema.level_1,apipostschema.level_2,apipostschema.level_3,apipostschema.category_text)
    except Exception as e:
        print(e)
        return None

@router.post("/suppliers/get-supplier-details")
async def search_suppliers_get_supplier_details_api_fun(apipostschema:SupplierDetails):
    try:
        set_env_var()
        new_dbobj=database()
        return supplier_details_api(new_dbobj,sqlSchemaName,apipostschema.supplier_id)
    except Exception as e:
        print(e)
        return None