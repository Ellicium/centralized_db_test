import os
import logging
from fastapi import FastAPI, Request, Form, Response, APIRouter
from fastapi.logger import logger
from dotenv import load_dotenv
from ..config.db_config import database
from ..schemas.update_schema import UpdateSupplier,getSupplierContact
from ..services.update_service import set_supplier_details_func,get_supplier_contact_func

gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)
load_dotenv()

router = APIRouter()

dbobj=database()

@router.post("/suppliers/set-supplier-details-info")
async def set_supplier_details_info(apipostschema:UpdateSupplier):
    try:
        data= set_supplier_details_func(apipostschema,dbobj)
        return data
    except Exception as e:
        logger.error("get_supplier_count failed",exc_info=e)
@router.post("/suppliers/get-supplier-contact")
async def get_supplier_contact(apipostschema:getSupplierContact):
    try:
        data= get_supplier_contact_func(apipostschema,dbobj)
        return data
    except Exception as e:
        logger.error("get_supplier_count failed",exc_info=e)