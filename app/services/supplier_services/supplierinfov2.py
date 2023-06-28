import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from ...config.logger_config import get_uvicorn_logger,get_gunicorn_logger
from ...utils.timer import timer_func
from ...services.supplier_service import clean_main

load_dotenv()
logger = get_gunicorn_logger()

@timer_func
def hit_azuresearch_api(payload):
    try:
        logger.info(f'hit_azuresearch_api payload: {payload}')
        headers = {
            'Accept': 'application/json',
            'api-key': os.getenv('AzureCognitiveSearchKey'),
            'Content-Type': 'application/json'
        }
        url = os.getenv("AzureCognitiveSearchURL")
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()  # Raise an exception if the response indicates an error
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error("An error occurred while making the hit_azuresearch_api request", exc_info=e)
    except Exception as e:
        logger.error("An unexpected error occurred", exc_info=e)

@timer_func
def get_supplier_information_service(freetext1:None , country:None, page_number:0, page_size:20,preferred_flag:0):
    try:
        payload_dict = {}
        return_dict = {}
        if (freetext1 == ""):
            freetext1 = "*"
        else:
            freetext1=clean_main(freetext1)
        # if len(country[0])>0:
        if len(country) > 0:
            country_filter = f"Country_Region eq '{country[0].lower()}'"
            payload_dict["filter"] = country_filter
        if (preferred_flag==1) and (len(country)==0):
            payload_dict["filter"] = f"key_contact_name ne null and email ne null"
        # populating payload
        payload_dict["search"] = f"{freetext1}"
        payload_dict["searchFields"] = "Supplier_ID,Supplier_Name,Supplier_Capability,Level_1,Level_2,Level_3"
        payload_dict["count"] = "true"
        payload_dict["select"] = "id,Supplier_ID, Supplier_Name, Country_Region, Supplier_Capability, Level_1, Level_2, Level_3, email,  ap_preferred, supplier_additional_info, website, phone, key_contact_name"
        payload_dict["top"] = page_size
        payload_dict["skip"] = (page_number - 1) * page_size
        payload_dict["searchMode"] = "all"
        # call azuresearch api with payload
        data = hit_azuresearch_api(payload_dict)
        response_dict = json.loads(data)
        total_record = response_dict["@odata.count"]
        dict_data = response_dict["value"]
        return_dict["data"] = dict_data
        return_dict["total_record"] = total_record
        return return_dict
    except Exception as e:
        logger.error("get_supplier_information failed",exc_info=e)


@timer_func
def get_supplier_information_rfi_service(freetext1:None , country:None, page_number:0, page_size:20,preferred_flag:0,supplier_list:None,skip_supplier_list:None):
    try:
        payload_dict = {}
        return_dict = {}
        if (freetext1 == ""):
            freetext1 = "*"
        else:
            freetext1=clean_main(freetext1)
        # if len(country[0])>0:
        if (supplier_list):
            index = 0
            page_size=1000000
            page_number=0
            for x in supplier_list:
                if index==0:
                    payload_dict["filter"]=f"id eq {x}"
                else:
                    payload_dict["filter"]+=f" or id eq {x}"
                index += 1
        elif (skip_supplier_list):
            index = 0
            print('skipping')
            for x in skip_supplier_list:
                if index==0:
                    payload_dict["filter"]=f"id ne {x}"
                else:
                    payload_dict["filter"]+=f" and id ne {x}"
                index += 1
        else:
            payload_dict["filter"]=f"key_contact_name ne null"
        
        if len(country) > 0:
            country_filter = f" and Country_Region eq '{country[0].lower()}'"
            payload_dict["filter"] += country_filter
        if (preferred_flag==1) and (len(country)==0):
            payload_dict["filter"] += f" and key_contact_name ne null and email ne null"
    
        # populating payload
        payload_dict["search"] = f"{freetext1}"
        payload_dict["searchFields"] = "Supplier_ID,Supplier_Name,Supplier_Capability,Level_1,Level_2,Level_3"
        payload_dict["count"] = "true"
        payload_dict["select"] = "id,Supplier_ID, Supplier_Name, Country_Region, Supplier_Capability, Level_1, Level_2, Level_3, email,  ap_preferred, supplier_additional_info, website, phone, key_contact_name"
        payload_dict["top"] = page_size
        payload_dict["skip"] = page_number
        payload_dict["searchMode"] = "all"
        # call azuresearch api with payload
        print(payload_dict)
        data = hit_azuresearch_api(payload_dict)
        response_dict = json.loads(data)
        total_record = response_dict["@odata.count"]
        dict_data = response_dict["value"]
        return_dict["data"] = dict_data
        return_dict["total_record"] = total_record
        return return_dict
    except Exception as e:
        logger.error("get_supplier_information failed",exc_info=e)