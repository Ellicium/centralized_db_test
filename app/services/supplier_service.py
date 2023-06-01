import os
import math
import logging
import pandas as pd
from time import time
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

def get_filters(dbobj):
    try:
        set_env_var()
        #dbobj=database(sqlUserName , sqlPassword , sqlDatabaseName , sqlServerName)
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

def countrywise_supplier_count(freetext, level_1, level_2,level_3, dbobj):
    try:
        set_env_var()
        #dbobj=database(sqlUserName , sqlPassword , sqlDatabaseName , sqlServerName)
        apiquery = f"""SELECT distinct
                count(query1.supplier_id) as 'supplier_count',  
                upper(query1.country) as 'country',
                upper(query1.iso2) 'country_code'
            FROM
                (SELECT distinct
                    ds.name,
                    asm.supplier_id,
                    dc.country,
                    dc.iso2
                FROM
                    {sqlSchemaName}.address_supplier_mapping asm
                    LEFT JOIN {sqlSchemaName}.dim_supplier ds ON asm.supplier_id = ds.id
                    LEFT JOIN {sqlSchemaName}.dim_address da ON asm.address_id = da.id
                    LEFT JOIN {sqlSchemaName}.dim_country dc ON da.country_id = dc.id
                WHERE dc.country is not null and dc.iso2 is not null) query1
            Inner JOIN
                (SELECT distinct
                    csm.supplier_id,
                    dc1.name level_1,
                    dc2.name level_2,
                    dc3.name level_3
                FROM
                    {sqlSchemaName}.category_supplier_mapping csm
                    LEFT JOIN {sqlSchemaName}.dim_supplier ds ON csm.supplier_id = ds.id
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
            # if flag:
            #     apiquery += " WHERE"
            #     flag = False
            # else:
            #     apiquery += " AND"

        #apiquery += " query1.country is not null and iso2 is not null"
        apiquery += " group by query1.country, query1.iso2 order by supplier_count desc;"
        print(apiquery)
        countrywiseSupplierCount = dbobj.read_table(apiquery)
        return countrywiseSupplierCount
    except Exception as e:
        logger.error("countrywise_supplier_count failed",exc_info=e)



def return_nullif_none(supplier,category,region,level_1,level_2,level_3,text):
    if str(supplier).lower().strip()=='none':
        supplier=None
    if str(category).lower().strip()=='none':
        category=None
    if str(region).lower().strip()=='none':
        region=None
    if str(level_1).lower().strip()=='none':
        level_1=None
    if str(level_2).lower().strip()=='none':
        level_2=None
    if str(level_3).lower().strip()=='none':
        level_3=None
    if str(text).lower().strip()=='none':
        text=None
    
    return supplier,category,region,level_1,level_2,level_3,text
        

def search_suppliers_get_suppliers_information(new_dbobj,sqlSchemaName,supplier,category,region,level_1,level_2,level_3,text):
    try:
        supplier,category,region,level_1,level_2,level_3,text=return_nullif_none(supplier,category,region,level_1,level_2,level_3,text)
        return_dict={}
        
        supplier_info_query=f'''select distinct
    ds.ap_supplier_id,
    ds.name as Supplier_Name , 
    dc2.country ,
    dsi.Supplier_Capability,
    dcc4.name as level1,
    dcc5.name as level2,
    dcc6.name as level3
    from {sqlSchemaName}.dim_supplier ds 
    left join {sqlSchemaName}.dim_supplier_info dsi
    on ds.id= dsi.supplier_id 
    left join {sqlSchemaName}.address_supplier_mapping asm 
    on asm.supplier_id = ds.id 
    left join {sqlSchemaName}.dim_address dc 
    on asm.address_id = dc.id 
    left join {sqlSchemaName}.dim_country dc2 
    on dc.country_id = dc2.id 
    left join {sqlSchemaName}.category_supplier_mapping csm 
    on csm.supplier_id =ds.id 
    left join {sqlSchemaName}.dim_category_level dcl 
    on dcl.id = csm.category_level_id 
    left join {sqlSchemaName}.dim_category dcc4
    on dcc4.id =dcl.level_1_category_id 
    left join {sqlSchemaName}.dim_category dcc5
    on dcc5.id =dcl.level_2_category_id
    left join {sqlSchemaName}.dim_category dcc6
    on dcc6.id =dcl.level_3_category_id
    '''

        flag=any((supplier,category,region,level_1,level_2,level_3,text))

        already_condition_exist=0
        
        if flag:
            supplier_info_query+=' where '        
            if supplier:
                if already_condition_exist==1:
                    supplier_info_query+=' and '            

                supplier_info_query+=" ds.name = '"+str(supplier)+"'"
                already_condition_exist=1

            if region:
                if already_condition_exist==1:
                    supplier_info_query+=' and '            

                supplier_info_query+=" dc2.country = '"+str(region)+"'"
                already_condition_exist=1

            if category:
                if already_condition_exist==1:
                    supplier_info_query+=' and '            

                supplier_info_query+=" dsi.Key_Categories = '"+str(category)+"'"
                already_condition_exist=1

            if level_1:
                if already_condition_exist==1:
                    supplier_info_query+=' and '            

                supplier_info_query+=" dcc4.name = '"+str(level_1)+"'"
                already_condition_exist=1

            if level_2:
                if already_condition_exist==1:
                    supplier_info_query+=' and '            

                supplier_info_query+=" dcc5.name = '"+str(level_2)+"'"
                already_condition_exist=1

            if level_3:
                print('level_3_type',type(level_3))
                if already_condition_exist==1:
                    supplier_info_query+=' and '            

                supplier_info_query+=" dcc6.name = '"+str(level_3)+"'"
                already_condition_exist=1


            if text:
                if already_condition_exist==1:
                    supplier_info_query+=' and '            


                supplier_info_query+=f''' LOWER(ds.name) like '{text}'
                          or LOWER(dcc4.name) like '{text}'
                          or LOWER(dcc5.name) like '{text}' 
                          or LOWER(dcc6.name) like '{text}' '''            

                already_condition_exist=1
        
        supplier_info_query+=';'

        print(supplier_info_query)

        suppliers_df=new_dbobj.read_table(supplier_info_query)

        suppliers_df_data=suppliers_df.to_dict('records')    
        
        return_dict['data']=suppliers_df_data

        filter_query=f'''select
        count(*) as total_record
        from {sqlSchemaName}.dim_supplier ds
        left join {sqlSchemaName}.category_supplier_mapping csm
        on csm.supplier_id =ds.id
        left join {sqlSchemaName}.dim_category_level dcl
        on dcl.id = csm.category_level_id
        left join {sqlSchemaName}.dim_category dcc4
        on dcc4.id =dcl.level_1_category_id
        left join {sqlSchemaName}.dim_category dcc5
        on dcc5.id =dcl.level_2_category_id
        left join {sqlSchemaName}.dim_category dcc6
        on dcc6.id =dcl.level_3_category_id
        '''
        filter_query_flag=any((level_1,level_2,level_3,text))

        already_condition_exist_filter=0

        if filter_query_flag:
            filter_query+=' where '

            if level_1:
                if already_condition_exist_filter==1:
                    filter_query+=' and '            

                filter_query+=" dcc4.name = '"+str(level_1)+"'"
                already_condition_exist_filter=1

            if level_2:
                if already_condition_exist_filter==1:
                    filter_query+=' and '            

                filter_query+=" dcc5.name = '"+str(level_2)+"'"
                already_condition_exist_filter=1

            if level_3:
                if already_condition_exist_filter==1:
                    filter_query+=' and '            

                filter_query+=" dcc6.name = '"+str(level_3)+"'"
                already_condition_exist_filter=1

            if text:
                if already_condition_exist_filter==1:
                    filter_query+=' and '            

                filter_query+=f''' LOWER(ds.name) like '{text}'
                          or LOWER(dcc4.name) like '{text}'
                          or LOWER(dcc5.name) like '{text}' 
                          or LOWER(dcc6.name) like '{text}' '''            
                already_condition_exist_filter=1

        filter_query+=';'        
        print(filter_query)
        filter_query_df=new_dbobj.read_table(filter_query)
        return_dict['Total_record']=list(filter_query_df['total_record'])[0]
        return return_dict
    except:
        return None


def return_null_if_none_category(level_1,level_2,level_3,category_text):
    if str(level_1).lower().strip()=='none':
        level_1=None
    if str(level_2).lower().strip()=='none':
        level_2=None
    if str(level_3).lower().strip()=='none':
        level_3=None
    if str(category_text).lower().strip()=='none':
        category_text=None
    
    return level_1,level_2,level_3,category_text
    


def get_categorywise_count(new_dbobj,schema_name, level_1,level_2,level_3,category_text):
    try:
        response_dict={}

        level_1,level_2,level_3,category_text=return_null_if_none_category(level_1,level_2,level_3,category_text)
        
        filter_query=' '

        if level_1:
            filter_query+=f" and dcc4.name like '{level_1}' "
        if level_2:
            filter_query+=f" and dcc5.name like '{level_2}' "
        if level_3:
            filter_query+=f" and dcc6.name like '{level_3}' "
        if category_text:
            filter_query+=f" and dcc6.name like '{category_text}' or and dcc5.name like '{category_text}' or and dcc4.name like '{category_text}' "

        query_data=f'''
          select DISTINCT 
            dcc4.name as category ,
            count(DISTINCT ds.id) as category_count
            from {schema_name}.dim_supplier ds
            left join {schema_name}.category_supplier_mapping csm
            on csm.supplier_id =ds.id
            left join {schema_name}.dim_category_level dcl
            on dcl.id = csm.category_level_id
            left join {schema_name}.dim_category dcc4
            on dcc4.id =dcl.level_1_category_id
            left join {schema_name}.dim_category dcc5
            on dcc5.id =dcl.level_2_category_id
            left join {schema_name}.dim_category dcc6
            on dcc6.id =dcl.level_3_category_id
            where dcc4.name is not null
            {filter_query}
            group by dcc4.name 
           order by count(DISTINCT ds.id) desc;'''
        
        unique_supplier_count_query=f'''select count(DISTINCT name) as supplier_name from {schema_name}.dim_supplier ds where name is not null;'''
        print(query_data)
        data_df = new_dbobj.read_table(query_data)
        supplier_count_df = new_dbobj.read_table(unique_supplier_count_query)
        response = data_df.to_dict(orient='records')
        response_dict['data']=response
        response_dict['Total_Catagory']=str(math.floor(supplier_count_df['supplier_name'][0]/1000))+'K'
        return response_dict
    except Exception as e:
        print(e)
        return None
    

def supplier_details_api(new_dbobj,schema_name,supplier_id):
    try:
        supplier_id=str(int(supplier_id))
        # if supplier_id=='0':
        #     supplier_id=None
        sql_query_for_data_for_supplier_id=f'''select
        q3.Supplier_Name ,q1.level1 ,q1.level2,q1.level3,q2.supplier_additional_info,q2.supplier_capability ,q2.additionalNotes,q3.country ,q3.address ,q3.email ,q3.website,q3.phone
        from
        (
        select
            DISTINCT 
        ds.id,
            dcc4.name as level1,
            dcc5.name as level2,
            dcc6.name as level3
        from
            {schema_name}.dim_supplier ds
        left join {schema_name}.category_supplier_mapping csm
        on
            csm.supplier_id = ds.id
        left join {schema_name}.dim_category_level dcl
        on
            dcl.id = csm.category_level_id
        left join {schema_name}.dim_category dcc4
        on
            dcc4.id = dcl.level_1_category_id
        left join {schema_name}.dim_category dcc5
        on
            dcc5.id = dcl.level_2_category_id
        left join {schema_name}.dim_category dcc6
        on
            dcc6.id = dcl.level_3_category_id
        where
            ds.id = {supplier_id}) q1
        join 
        (
        select
            DISTINCT ds.id,
            dsi.supplier_additional_info,
            dsi.supplier_capability ,
            null as additionalNotes
        from
            {schema_name}.dim_supplier ds
        left join {schema_name}.dim_supplier_info dsi
        on
            ds.id = dsi.supplier_id
        where
            ds.id = {supplier_id}) q2
        on
        q1.id = q2.id
        join
        (
        select
            DISTINCT
        ds.id,
            ds.name as Supplier_Name ,    dc2.country ,    dc.address ,    dc3.email ,    dc3.website,    dc3.phone
        from
            {schema_name}.dim_supplier ds
        left join {schema_name}.dim_supplier_info dsi
        on
            ds.id = dsi.supplier_id
        left join {schema_name}.address_supplier_mapping asm
        on
            asm.supplier_id = ds.id
        left join {schema_name}.dim_address dc
        on
            asm.address_id = dc.id
        left join {schema_name}.dim_country dc2
        on
            dc.country_id = dc2.id
        left join {schema_name}.dim_contact dc3 
        on
            dc3.supplier_id = ds.id
            and dc3.address_supplier_mapping_id = asm.address_id
        where
            ds.id = {supplier_id}
        ) q3
        on
        q2.id = q3.id
        ;
        '''
        supplier_info_df = new_dbobj.read_table(sql_query_for_data_for_supplier_id)
        return supplier_info_df.to_dict(orient='records')
    except Exception as e:
        print(e)
        return None
    
