import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from...config.logger_config import get_logger

load_dotenv()
logger = get_logger()


def hit_azuresearch_api(payload):
    try:
        headers = {
                    'Accept': 'application/json',
                    'api-key': os.getenv('AzureCognitiveSearchKey'),
                    'Content-Type': 'application/json'
                    }
        url = os.getenv("AzureCognitiveSearchURL")
        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
        return response.text
    except Exception as e:
        logger.error("hit_azuresearch_api failed",exc_info=e)

def get_supplier_information_service(freetext1:None , country:None, page_number:0, page_size:20):
    try:
        payload_dict = {}
        return_dict = {}
        if freetext1 is None:
            freetext1 = "*"
        if len(country[0])>0:
            country_filter = f"Country_Region eq '{country[0]}'"
            payload_dict['filter'] = country_filter
        # populating payload
        payload_dict["search"] = f"{freetext1}"
        payload_dict["searchFields"] = "Supplier_ID,Supplier_Name,Supplier_Capability,Level_1,Level_2,Level_3"
        payload_dict["count"] = "true"
        payload_dict["top"] = page_size
        payload_dict["skip"] = page_number - 1
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