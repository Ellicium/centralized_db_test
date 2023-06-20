import os
import math
import logging
import pandas as pd
pd.set_option('display.max_columns', None)
from time import time
import datetime
from dotenv import load_dotenv
from fastapi.logger import logger

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
def get_supplier_contact_func(object,dbobj):
    try:
        set_env_var()
        
        lev1=f'''select asm.id as 'address_supplier_mapping_id' ,da.address , 
(SELECT city from {sqlSchemaName}.dim_city where id=da.city_id) as 'city',
(SELECT state from {sqlSchemaName}.dim_state ds  where id=da.state_id) as 'state',
(SELECT country from {sqlSchemaName}.dim_country where id=da.country_id) as 'country',
(SELECT iso2 from {sqlSchemaName}.dim_country where id=da.country_id) as 'iso2'
from {sqlSchemaName}.address_supplier_mapping asm 
inner join {sqlSchemaName}.dim_address da on asm.address_id = da.id 
where asm.supplier_id = {object.Shipper_Id}'''
        lev2=f'''SELECT dc.address_supplier_mapping_id,dc.id as 'contact_id' ,dc.phone ,dc.email ,dc.website 
from {sqlSchemaName}.dim_contact dc 
WHERE dc.supplier_id = {object.Shipper_Id}'''
        level1Data = dbobj.read_table(lev1)
        level2Data = dbobj.read_table(lev2)
#         level2Data = level2Data.groupby('address_supplier_mapping_id').agg({
#     'phone': lambda x: list(set(x)),
#     'email': lambda x: list(set(x)),
#     'website': 'first'
# }).reset_index()
        contact_dict = {}
        for index, row in level2Data.iterrows():
            address_id = row['address_supplier_mapping_id']
            contact_data = {
                'contact_id': row['contact_id'],
                'phone': row['phone'],
                'email': row['email'],
                'website': row['website']
            }
            if address_id not in contact_dict:
                contact_dict[address_id] = []
            contact_dict[address_id].append(contact_data)

        # Create a DataFrame from the contact_dict
        contact_df = pd.DataFrame(contact_dict.items(), columns=['address_supplier_mapping_id', 'contact_details'])
        merged_df = contact_df.merge(level1Data, on='address_supplier_mapping_id')
        return merged_df.to_dict(orient='records')
        
    except Exception as e:
        logger.error("get_filters def failed",exc_info=e)