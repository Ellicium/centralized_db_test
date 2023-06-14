import os
import math,datetime,pycountry
import logging,re
import pandas as pd
from time import time
from dotenv import load_dotenv
from fastapi.logger import logger
import spacy
# from textblob import TextBlob
nlp = spacy.load('en_core_web_sm')
import logging
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



def search_suppliers_get_suppliers_information(new_dbobj,supplier,region,page_number,page_size,preffered_flag):
    try:
        set_env_var()
        if supplier:
            supplier=clean_main(supplier)
        supplier,region=return_nullif_none(supplier,region)
        return_dict={}

        preferred_query= ''
        if preffered_flag==1:

            preferred_query='where ds.ap_preferred =1'

        supplier_info_query=f'''SELECT
	ds.id,
	ds.Supplier_ID,
	ds.Supplier_Name,
	ds.Country_Region,
	q2.Supplier_Capability,
	q2.Level_1,
	q2.Level_2,
	q2.Level_3,
	q2.email
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
		select distinct
			id,
			country
		from
			{sqlSchemaName}.dim_country dc3
)q3
on
		q3.id = dc.country_id
		{preferred_query}

)ds
inner join
(
	select distinct
		ds.id,
		dsi.Supplier_Capability,
		dcc4.name as Level_1,
		dcc5.name as Level_2,
		dcc6.name as Level_3,
		dc2.email
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
	left join {sqlSchemaName}.dim_contact dc2 
	on 
		dc2.supplier_id = ds.id 
	where dc2.email is not null
	and dsi.delete_flag is null
    and dc2.email not like 'none'
    
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
	q2.Level_3,
    q2.email    
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
(select distinct id,country from {sqlSchemaName}.dim_country dc3
where country in ( '{str("','".join(list(region))) }' ))q3
on q3.id=dc.country_id 
{preferred_query}
)ds
inner join 
(select distinct
	ds.id,
    dsi.Supplier_Capability,
    dcc4.name as Level_1,
    dcc5.name as Level_2,
    dcc6.name as Level_3,
    dc2.email
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
    	left join {sqlSchemaName}.dim_contact dc2 
	on 
		dc2.supplier_id = ds.id 
	where dc2.email is not null
	and dsi.delete_flag is null
    and dc2.email not like 'none'
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
            offset=None
            if page_number==1:
                offset=0
            else:
                offset=page_number
            if offset!=0:
                offset=(page_number-1)*page_size
            if offset<0:
                offset=0
            supplier_info_query+=f''' order by ds.id 
            offset {offset} rows
            FETCH next {page_size} rows only '''

        supplier_info_query+=';'

        print(supplier_info_query)

        suppliers_df=new_dbobj.read_table(supplier_info_query).drop_duplicates()#.drop(['id'], axis=1)

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
(select distinct id,country from {sqlSchemaName}.dim_country dc3
)q3
on q3.id=dc.country_id
{preferred_query}
)ds
inner join
(select distinct
        ds.id,
    dsi.Supplier_Capability,
    dcc4.name as Level_1,
    dcc5.name as Level_2,
    dcc6.name as Level_3,
    dc2.email
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
    	left join {sqlSchemaName}.dim_contact dc2 
	on 
		dc2.supplier_id = ds.id 
	where dc2.email is not null
	and dsi.delete_flag is null
    and dc2.email not like 'none'
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

def get_datetime_attr(pandas_dff):
    start = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    pandas_dff=pandas_dff.drop_duplicates()
    pandas_dff['created_date']=start
    pandas_dff['created_by']=None
    pandas_dff['updated_date']=start
    pandas_dff['updated_by']=None
    return pandas_dff

def get_filter_sql_query(schema_name,table_name,data_dataframe):
    data_dataframe_dict=data_dataframe.to_dict(orient='records')
    sql_query=f'''select id from {schema_name}.{table_name} '''

    filter_query =''
    for dictt in data_dataframe_dict:
        for key in dictt:
            if (dictt[key]):
                if filter_query!='':
                    filter_query+=' and '
                else:
                    filter_query+=' where '
                filter_query+=f" {key}='{dictt[key]}' "
    sql_query+=filter_query+' ;'
    if filter_query!='':
        return sql_query
    return None

def soft_delete_function(new_dbobj,table_name,schema_name,data_dataframe):
    if str(table_name).strip().lower()=='dim_supplier_info':
        soft_delete_query=f''' update {schema_name}.dim_supplier_info set delete_flag = 1 where supplier_id = {data_dataframe['supplier_id'][0]}'''
        new_dbobj.execute_query(soft_delete_query)
    # elif str(table_name).strip().lower()=='dim_supplier_info':

def get_normalized_id(new_dbobj,table_name,schema_name,data_dataframe):
    mapping = {country.name.lower().strip(): country.alpha_2 for country in pycountry.countries}
    sql_query = get_filter_sql_query(schema_name,table_name,data_dataframe)
    data_dataframe = get_datetime_attr(data_dataframe)
    sql_data = new_dbobj.read_table(sql_query)
    if len(sql_data)==0:
        soft_delete_function(new_dbobj,table_name,schema_name,data_dataframe)
        new_dbobj.insert_data(data_dataframe, table_name ,schema_name)
        sql_data = new_dbobj.read_table(sql_query)
    return sql_data['id'][0]
    
def insert_suppliers_data_fun(new_dbobj,input_payload):
        set_env_var()
        input_payload_list=input_payload

        contact_input_df=pd.DataFrame.from_dict(input_payload_list)
        contact_input_df_copy=contact_input_df.copy()

        for df_len_itr in range(len(contact_input_df_copy)):
            contact_input_df=contact_input_df_copy[df_len_itr:df_len_itr+1].reset_index().drop(['index'], axis=1)
            print(contact_input_df)

            for column in contact_input_df:
                if column not in ['Pin_Code','Phone','ap_preffered_supplier']:
                    contact_input_df[column]=contact_input_df[column].str.lower()
            
            contact_input_df['ap_preffered_supplier']=contact_input_df.ap_preffered_supplier.map(dict(yes=1, no=0))

            for column in contact_input_df.columns:
                if not  contact_input_df[column][0]:
                    contact_input_df=contact_input_df.drop([column], axis=1)
            
            supplier_df=contact_input_df[['supplier_name']].rename(columns = {'supplier_name':'name'})
            contact_input_df['supplier_id'] = get_normalized_id(new_dbobj,'dim_supplier',sqlSchemaName,supplier_df)
            
            city_df = contact_input_df[['City']]
            city_id = get_normalized_id(new_dbobj,'dim_city',sqlSchemaName,city_df)

            state_df = contact_input_df[['State']]
            state_id = get_normalized_id(new_dbobj,'dim_state',sqlSchemaName,state_df)

            country_df = contact_input_df[['Country']].rename(columns = {'country_code':'iso2'})
            country_id = get_normalized_id(new_dbobj,'dim_country',sqlSchemaName,country_df)

            contact_input_df['city_id'] = city_id
            contact_input_df['state_id'] = state_id
            contact_input_df['country_id'] = country_id
            # contact_input_df.rename(columns = {'Pin_Code':'pincode'}, inplace = True)
            database_insert_df=contact_input_df[['address',  'city_id', 'state_id', 'country_id']]
            address_id=get_normalized_id(new_dbobj,'dim_address',sqlSchemaName,database_insert_df)
            address_supplier_mapping=contact_input_df[['supplier_id']]
            address_supplier_mapping['address_id']=address_id
            address_supplier_mapping_id = get_normalized_id(new_dbobj,'address_supplier_mapping',sqlSchemaName,address_supplier_mapping)
            
            contact_df = contact_input_df[['Email', 'Phone', 'website','supplier_id']]
            contact_df.rename(columns = {'Email':'email','Phone':'phone'}, inplace = True)
            contact_df['address_supplier_mapping_id']=address_supplier_mapping_id

            contact_id = get_normalized_id(new_dbobj,'dim_contact',sqlSchemaName,contact_df)
            
            supplier_info_df=contact_input_df[['supplier_capability','supplier_additional_info','supplier_id']]
            supplier_info_id = get_normalized_id(new_dbobj,'dim_supplier_info',sqlSchemaName,supplier_info_df)
            
        return 'API Execution Successful'
    

def get_all_suppliers_data_fun(new_dbobj,supplier_id_list):
    try:
        set_env_var()
        supplier_id_list = map(str, supplier_id_list)
        schema_name=sqlSchemaName
        # supplier_id=str(int(supplier_id))
        supplier_id=f''' ('{str("','".join(list(supplier_id_list))) }') '''
        
        sql_query_for_data_for_supplier_id=f'''select
        q3.supplier_name as Supplier_Name,q2.ap_supplier_id,q1.level1 as Level_1,q1.level2 as Level_2,q1.level3 as Level_3,q2.supplier_additional_info as Supplier_Additional_Info,q2.supplier_capability as Supplier_Capability,q3.country as Country_Region,q3.address as Address,q3.email as Email,q3.website as Website,q3.phone as Phone,q3.key_contact_name 
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
            ds.id in {supplier_id}) q1
        join 
        (
        select
            DISTINCT ds.id,
            ds.ap_supplier_id,
            dsi.supplier_additional_info,
            dsi.supplier_capability ,
            null as additionalNotes
        from
            {schema_name}.dim_supplier ds
        left join {schema_name}.dim_supplier_info dsi
        on
            ds.id = dsi.supplier_id
        where
            ds.id in {supplier_id}
            )q2
        on
        q1.id = q2.id
        join
        (
        select
            DISTINCT
        ds.id,
            ds.name as Supplier_Name ,    dc2.country ,    dc.address ,    dc3.email ,    dc3.website,    dc3.phone , dc3.key_contact_name
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
            and dc3.address_supplier_mapping_id = asm.id
        where
            ds.id in {supplier_id}
        ) q3
        on
        q2.id = q3.id
        ;
        '''
        print(sql_query_for_data_for_supplier_id)
        supplier_info_df = new_dbobj.read_table(sql_query_for_data_for_supplier_id)
        return supplier_info_df.to_dict(orient='records')
    except Exception as e:
        logger.error(e)
        print(e)
        return None


def extract_entities(text):
    try:
        doc = nlp(text)
        entities = []
        # print(doc.ents)
        for ent in doc.ents:
            # print(ent)
            if ent.label_ in ['GPE', 'LOC']:  # Filter entities labeled as geographical or location entities
                entities.append(ent.text)
        logger.info('Location extraction from free search complete')
        # print('Location extraction from free search complete')
        return entities
    except Exception as e:
        print('Location Extraction from free search failed',e)
        logger.error('Location Extraction from free search failed',e)



def remove_substrings(string, substrings):
    pattern = '|'.join(map(re.escape, substrings))
    return re.sub(pattern, '', string)

def find_industry_in_text(result):
    industry_list = result.split(",")
    industry_list = [item.strip() for item in industry_list if item.strip()]
    return industry_list

def loc_industry_standardisation(country_,industry_names):
    user_input_industry_data=pd.DataFrame()
    user_input_country_data=pd.DataFrame()
    user_input_industry_data['industry']=industry_names
    user_input_industry_data=user_input_industry_data.sort_values(by=['industry'])
    user_input_country_data['country']=country_
    user_input_country_data=user_input_country_data.sort_values(by=['country'])
    user_input_industry_data['key'] = 1
    user_input_country_data['key'] = 1
    user_input_data = pd.merge(user_input_industry_data, user_input_country_data, on='key').drop('key', axis=1)   
    print(user_input_data)
    return user_input_data



def clean_main(text):
    # Check if user input are valid
    # text=spell_check_input(text)
    country_=extract_entities(text)
    print(country_)
    if len(country_)==0:
        country_=[None]
        result=text
    else:
        result = remove_substrings(text, country_)
    industry_names=find_industry_in_text(result)
    print(country_,industry_names)
    user_input_data=loc_industry_standardisation(country_,industry_names)
    return user_input_data['industry'][0]

def update_contact_info_fun(new_dbobj,contact_df):
    
    if 'phone' in contact_df.columns and 'contact_id' in contact_df.columns :
        if contact_df['phone'][0] and contact_df['contact_id'][0]:
            sql_query_phone=f''' update {sqlSchemaName}.dim_contact set phone = '{contact_df['phone'][0]}' where id =  {contact_df['contact_id'][0]}'''
            print(sql_query_phone)
            new_dbobj.execute_query(sql_query_phone)

    if 'email' in contact_df.columns and 'contact_id' in contact_df.columns :
        if contact_df['email'][0] and contact_df['contact_id'][0]:
            sql_query_email=f''' update {sqlSchemaName}.dim_contact set email = '{contact_df['email'][0]}' where id =  {contact_df['contact_id'][0]}'''
            print(sql_query_email)
            new_dbobj.execute_query(sql_query_email)

    if 'website' in contact_df.columns and 'contact_id' in contact_df.columns :
        if contact_df['website'][0] and contact_df['contact_id'][0]:
            sql_query_website=f''' update {sqlSchemaName}.dim_contact set website = '{contact_df['website'][0]}' where id =  {contact_df['contact_id'][0]}'''
            print(sql_query_website)
            new_dbobj.execute_query(sql_query_website)

    if 'key_contact_name' in contact_df.columns and 'contact_id' in contact_df.columns :
        if contact_df['key_contact_name'][0] and contact_df['contact_id'][0]:
            sql_query_key_contact_name =f''' update {sqlSchemaName}.dim_contact set website = '{contact_df['key_contact_name'][0]}' where id =  {contact_df['contact_id'][0]}'''
            print(sql_query_key_contact_name)
            new_dbobj.execute_query(sql_query_key_contact_name)
    
    if 'person_role' in contact_df.columns and 'contact_id' in contact_df.columns :
        if contact_df['person_role'][0] and contact_df['contact_id'][0]:
            sql_query_person_role =f''' update {sqlSchemaName}.dim_contact set person_role = '{contact_df['person_role'][0]}' where id =  {contact_df['contact_id'][0]}'''
            print(sql_query_person_role)
            new_dbobj.execute_query(sql_query_person_role)
        
def update_address_info_fun(new_dbobj,address_dff):
    for column in address_dff.columns:
        ['address', 'address_id', 'city_id', 'state_id', 'country_id']
        if column!='address_id':
            addres_update_sql_query=f''' update {sqlSchemaName}.dim_address set {column}='{address_dff[column][0]}' where id = {address_dff['address_id'][0]}'''
            print(addres_update_sql_query)
            new_dbobj.execute_query(addres_update_sql_query)

    
def update_suppliers_contact_fun(new_dbobj,input_payload):
        set_env_var()
        print(input_payload)
        input_payload_list=[]
        input_payload_list.append(input_payload)

        contact_columns=[]
    
        contact_input_df=pd.DataFrame.from_dict(input_payload_list)
        contact_input_df_copy=contact_input_df.copy()

        for column in contact_input_df.columns:
            if column in ['contact_id', 'phone', 'email', 'website','key_contact_name','person_role']:
                contact_columns.append(column)
            
        contact_df=contact_input_df[contact_columns]

        update_contact_info_fun(new_dbobj,contact_df)
        print(contact_df)

        get_address_id_from_address_sipplier_mapping_query=f" select address_id from {sqlSchemaName}.address_supplier_mapping where id = {contact_input_df['address_supplier_mapping_id'][0]}"
        print(get_address_id_from_address_sipplier_mapping_query)
        address_id_df = new_dbobj.read_table(get_address_id_from_address_sipplier_mapping_query)
        print(address_id_df)
        contact_input_df['address_id']=address_id_df['address_id'][0]

        if 'city' in contact_input_df.columns:
            city_df = contact_input_df[['city']]
            city_id = get_normalized_id(new_dbobj,'dim_city',sqlSchemaName,city_df)

        if 'state' in contact_input_df.columns:
            state_df = contact_input_df[['state']]
            state_id = get_normalized_id(new_dbobj,'dim_state',sqlSchemaName,state_df)

        if 'country' in contact_input_df.columns:
            country_df = contact_input_df[['country','country_code']].rename(columns = {'country_code':'iso2'})
            country_id = get_normalized_id(new_dbobj,'dim_country',sqlSchemaName,country_df)

        if city_id:
            contact_input_df['city_id']=city_id

        if state_id:
            contact_input_df['state_id']=state_id
        
        if country_id:
            contact_input_df['country_id']=country_id
        
        contact_input_df_col=[]
        for column in ['address', 'address_id', 'city_id', 'state_id', 'country_id']:
            if column in contact_input_df.columns:
                contact_input_df_col.append(column)

        dim_address_df=contact_input_df[contact_input_df_col]
        update_address_info_fun(new_dbobj,dim_address_df)
                   
        return 'API Execution Successful'


def insert_suppliers_contact_fun(new_dbobj,input_payload):
        set_env_var()
        input_payload_list=[]
        input_payload_list.append(input_payload)

        contact_input_df=pd.DataFrame.from_dict(input_payload_list)
        contact_input_df_copy=contact_input_df.copy()

        for df_len_itr in range(len(contact_input_df_copy)):
            contact_input_df=contact_input_df_copy[df_len_itr:df_len_itr+1]

            for column in contact_input_df:
                if column not in ['Pin_Code','Phone','supplier_id']:
                    contact_input_df[column]=contact_input_df[column].str.lower()

            city_df = contact_input_df[['city']]
            city_id = get_normalized_id(new_dbobj,'dim_city',sqlSchemaName,city_df)

            state_df = contact_input_df[['state']]
            state_id = get_normalized_id(new_dbobj,'dim_state',sqlSchemaName,state_df)

            country_df = contact_input_df[['country','country_code']].rename(columns = {'country_code':'iso2'})
            country_id = get_normalized_id(new_dbobj,'dim_country',sqlSchemaName,country_df)

            contact_input_df['city_id'] = city_id
            contact_input_df['state_id'] = state_id
            contact_input_df['country_id'] = country_id
            contact_input_df.rename(columns = {'Pin_Code':'pincode'}, inplace = True)
            database_insert_df=contact_input_df[['address', 'pincode', 'city_id', 'state_id', 'country_id']]
            address_id=get_normalized_id(new_dbobj,'dim_address',sqlSchemaName,database_insert_df)
            address_supplier_mapping=contact_input_df[['supplier_id']]
            address_supplier_mapping['address_id']=address_id
            
            address_supplier_mapping_id = get_normalized_id(new_dbobj,'address_supplier_mapping',sqlSchemaName,address_supplier_mapping)
            
            contact_df = contact_input_df[['key_contact_name', 'Role','Email', 'Phone', 'website','supplier_id']]
            contact_df.rename(columns = {'Email':'email','Phone':'phone','Role':'person_role'}, inplace = True)
            contact_df['address_supplier_mapping_id']=address_supplier_mapping_id

            contact_id = get_normalized_id(new_dbobj,'dim_contact',sqlSchemaName,contact_df)
            
        return 'API Execution Successful'
    
