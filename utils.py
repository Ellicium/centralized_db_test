from time import time
import math

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

def get_categorywise_count(new_dbobj,schema_name):
    try:
        response_dict={}
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
            group by dcc4.name 
           order by count(DISTINCT ds.id) desc;
        '''

        unique_supplier_count_query=f'''select count(DISTINCT name) as supplier_name from {schema_name}.dim_supplier ds where name is not null;'''
        data_df = new_dbobj.read_table(query_data)
        supplier_count_df = new_dbobj.read_table(unique_supplier_count_query)
        response = data_df.to_dict(orient='records')
        response_dict['data']=response
        response_dict['Total_Catagory']=str(math.floor(supplier_count_df['supplier_name'][0]/1000))+'K'
        return response_dict
    except Exception as e:
        print(e)
        return None
        