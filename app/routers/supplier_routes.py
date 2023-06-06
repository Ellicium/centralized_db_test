import os
import logging
from fastapi import FastAPI, Request, Form, Response, APIRouter
from fastapi.logger import logger
from dotenv import load_dotenv
from ..config.db_config import database
from ..schemas.supplier_schema import SupplierCountPost,SupplierInfoCountry, SupplierCountResponse,FilterResponse,SupplierInfo ,SupplierInfoResponse,SupplierCategoryWise,SupplierDetails
from ..services.supplier_service import countrywise_supplier_count, get_categorywise_count, return_null_if_none_category,get_filters, search_suppliers_get_suppliers_information,supplier_details_api,get_unique_country

gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)
load_dotenv()


router = APIRouter()

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
async def search_suppliers_get_suppliers_information_api_fun(apipostschema:SupplierInfoCountry):
    try:
        # set_env_var()
        # new_dbobj=database()
        return search_suppliers_get_suppliers_information(dbobj,apipostschema.text,apipostschema.region,apipostschema.page_number,apipostschema.page_size)
    except Exception as e:
        logger.error(e)
        print(e)
        return None

@router.post("/suppliers/get-supplier-catogarywise")
async def search_suppliers_get_supplier_catogarywise_api_fun(apipostschema:SupplierCategoryWise):
    try:
        new_dbobj=database()
        return get_categorywise_count(new_dbobj,sqlSchemaName,apipostschema.level_1,apipostschema.level_2,apipostschema.level_3,apipostschema.category_text)
    except Exception as e:
        logger.error(e)
        print(e)
        return None

@router.post("/suppliers/get-supplier-details")
async def search_suppliers_get_supplier_details_api_fun(apipostschema:SupplierDetails):
    try:
        set_env_var()
        new_dbobj=database()
        return supplier_details_api(new_dbobj,sqlSchemaName,apipostschema.supplier_id)
    except Exception as e:
        logger.error(e)
        print(e)
        return None
    

@router.get("/suppliers/get_unique_country")
async def search_suppliers_get_unique_country():
    try:
        return get_unique_country(dbobj)
    except Exception as e:
        logger.error(e)
        print(e)
        return None
    

# @router.post("/suppliers/get-suppliers-information-country")
# async def search_suppliers_get_suppliers_information_by_country(apipostschema:SupplierInfoCountry):
#     try:
#         set_env_var()
#         new_dbobj=database()
#         return search_suppliers_get_suppliers_information(new_dbobj,sqlSchemaName,apipostschema.supplier,apipostschema.country,apipostschema.page_number,apipostschema.page_size)
#     except Exception as e:
#         logger.error(e)
#         print(e)
#         return None

