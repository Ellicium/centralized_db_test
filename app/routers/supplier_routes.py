import os
import logging
from fastapi import FastAPI, Request, Form, Response, APIRouter
from fastapi.logger import logger
from dotenv import load_dotenv
from ..config.db_config import database
from ..schemas.supplier_schema import SupplierCountPost,SupplierInfoCountry, SupplierCountResponse,FilterResponse,SupplierInfo ,SupplierInfoResponse,SupplierCategoryWise,SupplierDetails,UpdateSupplierDetails,allSupplierDetails,UpdateContactDetails,InsertContactDetails, SupplierInfoV2
from ..services.supplier_service import countrywise_supplier_count, get_categorywise_count, return_null_if_none_category,get_filters, search_suppliers_get_suppliers_information,supplier_details_api,get_unique_country,insert_suppliers_data_fun,get_all_suppliers_data_fun,update_suppliers_contact_fun,insert_suppliers_contact_fun

# v2
from ..services.supplier_services.supplierinfov2 import get_supplier_information_service


gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)
load_dotenv()

router = APIRouter()

dbobj=database()

@router.post("/suppliers/get-supplier-count", response_model=SupplierCountResponse)
async def get_supplier_count(apipostschema:SupplierCountPost):
    try:
        dbobj=database()
        logger.info(f"freetext:{apipostschema.text}, level_1:{apipostschema.level_1}, level_2:{apipostschema.level_2}, level_3:{apipostschema.level_3}")
        data = countrywise_supplier_count(apipostschema.text, apipostschema.level_1, apipostschema.level_2, apipostschema.level_3, dbobj)
        returnData = data.to_json(orient='records')
        return Response(content=returnData, media_type="application/json",status_code=200)
    except Exception as e:
        logger.error("get_supplier_count failed",exc_info=e)

@router.get("/suppliers/get-filters",response_model=FilterResponse)
async def get_filters_api():
    try:
        dbobj=database()
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
        dbobj=database()
        return search_suppliers_get_suppliers_information(dbobj,apipostschema.text,apipostschema.region,apipostschema.page_number,apipostschema.page_size,apipostschema.preffered_flag)
    except Exception as e:
        logger.error(e)
        print(e)
        return None


@router.post("/suppliers/get-supplier-details")
async def search_suppliers_get_supplier_details_api_fun(apipostschema:SupplierDetails):
    try:
        dbobj=database()
        return supplier_details_api(dbobj,apipostschema.supplier_id,apipostschema.supplier_name)
    except Exception as e:
        logger.error(e)
        print(e)
        return None
    

@router.get("/suppliers/get_unique_country")
async def search_suppliers_get_unique_country():
    try:
        dbobj=database()
        return get_unique_country(dbobj)
    except Exception as e:
        logger.error(e)
        print(e)
        return None
    
# @router.post("/suppliers/set-supplier-details")
# async def insert_suppliers_info(apipostschema:UpdateSupplierDetails):
#     try:
#         dbobj=database()
#         print(apipostschema.input_payload,type(apipostschema.input_payload))
#         return insert_suppliers_data_fun(dbobj,apipostschema.input_payload)
#     except Exception as e:
#         logger.error(e)
#         print(e)
#         return None
    
@router.post("/suppliers/get-all-suppliers-details")
async def all_Suppliers_details(apipostschema:allSupplierDetails):
    try:
        dbobj=database()
        return get_all_suppliers_data_fun(dbobj,apipostschema.supplier_id_list)
    except Exception as e:
        logger.error(e)
        print(e)
        return None


# @router.post("/suppliers/update-supplier-contact")
# async def update_suppliers_contact(apipostschema:UpdateContactDetails):
#     try:
#         print(apipostschema.input_payload,type(apipostschema.input_payload))
#         dbobj=database()
#         return update_suppliers_contact_fun(dbobj,apipostschema.input_payload)
#     except Exception as e:
#         logger.error(e)
#         print(e)
#         return None


# @router.post("/suppliers/insert-supplier-contact")
# async def insert_suppliers_contact(apipostschema:InsertContactDetails):
#     try:
#         print(apipostschema.input_payload,type(apipostschema.input_payload))
#         dbobj=database()
#         return insert_suppliers_contact_fun(dbobj,apipostschema.input_payload)
#     except Exception as e:
#         logger.error(e)
#         print(e)
#         return None



@router.post("/v2/suppliers/get-supplier-information")
async def get_supplier_infov2(apipostschema:SupplierInfoV2):
    try:
        logger.info(f"freetext:{apipostschema.text},region:{apipostschema.region},page_number:{apipostschema.page_number},page_size:{apipostschema.page_size},preferred_flag:{apipostschema.preferred_flag}")
        # service function call
        response_data = get_supplier_information_service(freetext1=apipostschema.text, country=apipostschema.region, page_number=apipostschema.page_number, page_size=apipostschema.page_size)
        return response_data
    except Exception as e:
        print(e)
        logger.error("get_supplier_infov2 failed",exc_info=e)