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
                        left join {sqlSchemaName}.dim_category dc on dcl.level_1_category_id = dc.id
                        order by dc.name'''
        level2Query = f'''select 
                    DISTINCT dc.name as 'label', dc.name as 'value' 
                    from {sqlSchemaName}.dim_category_level dcl  
                    left join {sqlSchemaName}.dim_category dc on dcl.level_2_category_id = dc.id
                    order by dc.name'''
        level3Query = f'''select 
                    DISTINCT dc.name as 'label', dc.name as 'value' 
                    from {sqlSchemaName}.dim_category_level dcl  
                    left join {sqlSchemaName}.dim_category dc on dcl.level_3_category_id = dc.id
                    order by dc.name'''
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



def return_nullif_none(supplier,region):
    if str(supplier).lower().strip()=='none':
        supplier=None
    if str(region).lower().strip()=='none':
        region=None
    
    return supplier,region
        

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
            dcc4.name as catagory ,
            count(DISTINCT ds.id) as catagory_count
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
        logger.error(e)
        print(e)
        return None
    

def supplier_details_api(new_dbobj,supplier_id,supplier_name):
    try:
        set_env_var()
        schema_name=sqlSchemaName
        supplier_id=str(int(supplier_id))
        # if supplier_id=='0':
        #     supplier_id=None
        sql_query_for_data_for_supplier_id=f'''select
        q3.supplier_name as Supplier_Name,q1.level1 as Level_1,q1.level2 as Level_2,q1.level3 as Level_3,q2.supplier_additional_info as Supplier_Additional_Info,q2.supplier_capability as Supplier_Capability,q3.country as Country_Region,q3.address as Address,q3.email as Email,q3.website as Website,q3.phone as Phone
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
            ds.id = {supplier_id}
            and 
            ds.name like '{supplier_name}') q2
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
        logger.error(e)
        print(e)
        return None
    

def get_unique_country(new_dbobj):
    try:
        set_env_var()
        schema_name=sqlSchemaName
        unique_country=new_dbobj.read_table(f'''select DISTINCT country,upper(iso2) as country_code  from {schema_name}.dim_country
        where country is not null;''')
        unique_country["country"]= unique_country["country"].str.title()
        return unique_country.to_dict(orient='records')
    except Exception as e:
        logger.error(e)
        print(e)
        return None



def search_suppliers_get_suppliers_information(new_dbobj,supplier,region,page_number,page_size):
    try:
        set_env_var()
        supplier,region=return_nullif_none(supplier,region)
        return_dict={}
        supplier_info_query=f'''SELECT
	ds.id,
	ds.Supplier_ID,
	ds.Supplier_Name,
	ds.Country_Region,
	q2.Supplier_Capability,
	q2.Level_1,
	q2.Level_2,
	q2.Level_3
from
	(
	select distinct
		ds.id,
		ds.ap_supplier_id as Supplier_ID,
		ds.name as Supplier_Name ,
		q3.country as Country_Region
	from
		{sqlSchemaName}.dim_supplier ds
	left join {sqlSchemaName}.dim_supplier_info dsi
on
		ds.id = dsi.supplier_id
	left join {sqlSchemaName}.address_supplier_mapping asm
on
		asm.supplier_id = ds.id
	left join {sqlSchemaName}.dim_address dc
on
		asm.address_id = dc.id
	left join
(
		select
			id,
			country
		from
			{sqlSchemaName}.dim_country dc3
)q3
on
		q3.id = dc.country_id
)ds
left join
(
	select distinct
		ds.id,
		dsi.Supplier_Capability,
		dcc4.name as Level_1,
		dcc5.name as Level_2,
		dcc6.name as Level_3
	from
		{sqlSchemaName}.dim_supplier ds
	left join {sqlSchemaName}.category_supplier_mapping csm
   on
		csm.supplier_id = ds.id
	left join {sqlSchemaName}.dim_category_level dcl
   on
		dcl.id = csm.category_level_id
	left join {sqlSchemaName}.dim_category dcc4
   on
		dcc4.id = dcl.level_1_category_id
	left join {sqlSchemaName}.dim_category dcc5
   on
		dcc5.id = dcl.level_2_category_id
	left join {sqlSchemaName}.dim_category dcc6
   on
		dcc6.id = dcl.level_3_category_id
	left join {sqlSchemaName}.dim_supplier_info dsi
   on
		dsi.supplier_id = ds.id
)q2
on
	ds.id = q2.id
    '''
        flag=any((supplier,region))

        already_condition_exist=0

        both_filter_flag=0

        if region and supplier:
            supplier_info_query=f'''SELECT 
	ds.id,
	ds.Supplier_ID,
    ds.Supplier_Name ,
    ds.Country_Region ,
	q2.Supplier_Capability,
	q2.Level_1,
	q2.Level_2,
	q2.Level_3    
from  
    (select distinct 
	ds.id,
	ds.ap_supplier_id as Supplier_ID,
    ds.name as Supplier_Name ,
    q3.country as Country_Region 
from {sqlSchemaName}.dim_supplier ds 
left join {sqlSchemaName}.dim_supplier_info dsi
on ds.id= dsi.supplier_id
left join {sqlSchemaName}.address_supplier_mapping asm
on asm.supplier_id = ds.id
left join {sqlSchemaName}.dim_address dc
on asm.address_id = dc.id
left join 
(select id,country from {sqlSchemaName}.dim_country dc3
where country in ( '{str("','".join(list(region))) }' ))q3
on q3.id=dc.country_id 
)ds
left join 
(select distinct
	ds.id,
    dsi.Supplier_Capability,
    dcc4.name as Level_1,
    dcc5.name as Level_2,
    dcc6.name as Level_3
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
    left join {sqlSchemaName}.dim_supplier_info dsi 
    on dsi.supplier_id = ds.id
)q2
on ds.id=q2.id
where '{str(supplier)}' in (Supplier_Name,Level_1,Level_2,Level_3,Supplier_ID,Supplier_Capability) and ds.Country_Region in ( '{str("','".join(list(region))) }' )'''

            both_filter_flag=1
            

        if flag and both_filter_flag==0:
            supplier_info_query+=' where '        
            if supplier:
                if already_condition_exist==1:
                    supplier_info_query+=' and '            

                supplier_info_query+=f''' '{str(supplier)}' in (q2.Level_1, q2.Level_2, q2.Level_3, q2.Supplier_Capability, ds.Supplier_ID, ds.Supplier_Name)'''
                already_condition_exist=1

            if region:
                if already_condition_exist==1:
                    supplier_info_query+=' and '            

                supplier_info_query+=" ds.Country_Region in ('"+str( "','".join(list(region)))+"')"
                already_condition_exist=1

        if page_size>0:
            offset=page_number-1*page_size
            if offset<0:
                offset=0
            supplier_info_query+=f''' order by ds.id 
            offset {offset} rows
            FETCH next {page_size} rows only '''

        supplier_info_query+=';'

        print(supplier_info_query)

        suppliers_df=new_dbobj.read_table(supplier_info_query).drop_duplicates()

        for column in suppliers_df:
            if column not in ['id','Supplier_ID']:
                suppliers_df[column]=suppliers_df[column].str.title()

        suppliers_df_data=suppliers_df.to_dict('records')    
        
        return_dict['data']=suppliers_df_data

        filter_query=f'''SELECT 
    count(*) as total_record
from
    (select distinct
        ds.id,
        ds.ap_supplier_id as Supplier_ID,
    ds.name as Supplier_Name ,
    q3.country as Country_Region
from {sqlSchemaName}.dim_supplier ds
left join {sqlSchemaName}.dim_supplier_info dsi
on ds.id= dsi.supplier_id
left join {sqlSchemaName}.address_supplier_mapping asm
on asm.supplier_id = ds.id
left join {sqlSchemaName}.dim_address dc
on asm.address_id = dc.id
left join
(select id,country from {sqlSchemaName}.dim_country dc3
)q3
on q3.id=dc.country_id
)ds
left join
(select distinct
        ds.id,
    dsi.Supplier_Capability,
    dcc4.name as Level_1,
    dcc5.name as Level_2,
    dcc6.name as Level_3
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
    left join {sqlSchemaName}.dim_supplier_info dsi
    on dsi.supplier_id = ds.id
)q2
on ds.id=q2.id
    '''

        flag=any((supplier,region))

        already_condition_exist=0
        
        if flag:
            filter_query+=' where '        
            if supplier:
                if already_condition_exist==1:
                    filter_query+=' and '            

                filter_query+=f''' '{str(supplier)}' in (q2.Level_1,q2.Level_2,q2.Level_3,q2.Supplier_Capability,ds.Supplier_ID,ds.Supplier_Name)'''
                already_condition_exist=1

            if region:
                if already_condition_exist==1:
                    filter_query+=' and '            

                filter_query+=f''' ds.Country_Region in  ('{str("','".join(list(region))) }') '''
                already_condition_exist=1
                
        print(filter_query)    
        filter_query_df=new_dbobj.read_table(filter_query)
        return_dict['total_record']=list(filter_query_df['total_record'])[0]
        return return_dict
    except Exception as e:
        logger.error(e)
        return None

