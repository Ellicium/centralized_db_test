import pandas as pd
import logging
import pyodbc,urllib,os,uvicorn,fastapi
from fastapi import FastAPI, Request, Form
from fastapi.logger import logger
from dotenv import load_dotenv
from sqlalchemy import create_engine

from fastapi.logger import logger
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

app = FastAPI(debug=True)

load_dotenv()

user_name=None
password=None
Database_Name=None
Server_Name=None
schema_name=None


class database:
    def __init__(self , SQLUser , SQLPassword , DatabaseName , ServerName ):
        driver="{ODBC Driver 17 for SQL Server}"
        self.conn = pyodbc.connect(f'''DRIVER={driver}; Server={ServerName }; 
                                UID={SQLUser}; PWD={SQLPassword}; DataBase={DatabaseName}''')
    
        server = ServerName
        database = DatabaseName
        user = SQLUser
        password = f"{SQLPassword}"
        driver = "{ODBC Driver 17 for SQL Server}"
        params = urllib.parse.quote_plus(f"""Driver={driver};
                                        Server=tcp:{server},1433;
                                        Database={database};
                                        Uid={user};Pwd={password};
                                        Encrypt=yes;
                                        TrustServerCertificate=no;
                                        Connection Timeout=30;""")
        self.conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(
            params)
        
    # Fetching the data from the selected table using SQL query
    def read_table(self,query):
        RawData= pd.read_sql_query(query, self.conn)
        return RawData
    
    def execute_query(self,query):
        cursor=self.conn.cursor()
        cursor.execute(query)
        cursor.commit()
        
    def close_conn_eng(self):
        self.conn.close()

    def insert_data(self, df, table_name ,schema_name):
        try :
            self.engine1 = create_engine(self.conn_str, fast_executemany=True)
            df.to_sql(table_name, con = self.engine1 ,if_exists='append', index=False,schema=schema_name)
        except Exception as e :
            print(e)    
            



def return_nullif_none(supplier,category,region,level_1,level_2,level_3,text,project_id):
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
    if str(project_id).lower().strip()=='none':
        project_id=None
    
    return supplier,category,region,level_1,level_2,level_3,text,project_id
        

def search_suppliers_get_suppliers_information(new_dbobj,schema_name,supplier,category,region,level_1,level_2,level_3,text,project_id):
    try:

        supplier,category,region,level_1,level_2,level_3,text,project_id=return_nullif_none(supplier,category,region,level_1,level_2,level_3,text,project_id)
        return_dict={}

        supplier_info_query=f'''select 
ds.id,
ds.name as Supplier_Name , 
dsi.Key_Categories,
dc2.country ,
dc3.Key_Contact_Name ,
dc3.Email, 
dc3.Website, 
dc3.Phone,
dc.address ,
dsi.Supplier_Capability, 
dsi.Supplier_Catalogue,
dcc4.name as level1,
dcc5.name as level2,
dcc6.name as level3
from {schema_name}.dim_supplier ds 
left join {schema_name}.dim_supplier_info dsi
on ds.id= dsi.supplier_id 
left join {schema_name}.address_supplier_mapping asm 
on asm.supplier_id = ds.id 
left join {schema_name}.dim_address dc 
on asm.address_id = dc.id 
left join {schema_name}.dim_country dc2 
on dc.country_id = dc2.id 
left join {schema_name}.dim_contact dc3 
on dc3.supplier_id = ds.id 
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
'''

        flag=any((supplier,category,region,level_1,level_2,level_3,text,project_id))

        already_condition_exist=0

        if flag:
            supplier_info_query+=' where'        
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

        suppliers_df=new_dbobj.read_table(supplier_info_query)
        
        suppliers_df_data=suppliers_df.to_dict('records')    
        
        return_dict['data']=suppliers_df_data

        filter_query=f'''select
count(*) as total_record
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

        filter_query_df=new_dbobj.read_table(filter_query)

        return_dict['Total_record']=list(filter_query_df['total_record'])[0]
        return return_dict
    except:
        return None

def get_top_regions_by_company(new_dbobj,schema_name,values):
    try:
        top_region_by_comp_query=f'''SELECT  TOP 5 afd.country_of_origin as Country_of_Origin, SUM(afd.weight) as Weight_Percentage , afd.company_group 
                FROM {schema_name}.app_faurecia_data afd  
                WHERE afd.company_group IN ('{values}')
                GROUP BY afd.country_of_origin, afd.company_group
                ORDER BY SUM(afd.weight) DESC '''
        top_region_by_comp=new_dbobj.read_table(top_region_by_comp_query)
        top_region_by_comp_data=top_region_by_comp.to_dict('records') 
        return top_region_by_comp_data
    except:
        return None

def get_distinct_company_group_names(new_dbobj,schema_name):
    try:
        dist_comp_query=f'''SELECT DISTINCT afd.company_group 
      FROM {schema_name}.app_faurecia_data afd'''
        distinct_company_group_df=new_dbobj.read_table(dist_comp_query)
        distinct_company_group_data=list(distinct_company_group_df['company_group'])
        return distinct_company_group_data
    except:
        return None



def set_env_var():
    global user_name,password,Database_Name,Server_Name,schema_name
    user_name=os.getenv("sqlUserName")
    password=os.getenv("sqlPassword")
    Database_Name=os.getenv("sqlDatabaseName")
    Server_Name=os.getenv("sqlServerName")
    schema_name=os.getenv("sqlSchemaName")

@app.get("/api/v1/supplier-market-analysis/get-filter")
async def supplier_market_analysis():
    try:
        set_env_var()
        new_dbobj=database(user_name , password , Database_Name , Server_Name)
        query = f'''SELECT DISTINCT Level_1 as label, Level_1 as value
        from {schema_name}.Vendors_Data vd WHERE Level_1 is not null '''

        query1 = f'''SELECT DISTINCT Level_2 as label, Level_2 as value
        from {schema_name}.Vendors_Data vd WHERE Level_2 is not null '''

        query2 = f'''SELECT DISTINCT Level_3 as label, Level_3 as value
        from {schema_name}.Vendors_Data vd WHERE Level_3 is not null'''

        new_df=new_dbobj.read_table(query)
        level_1=new_df.to_dict('records')

        new_df=new_dbobj.read_table(query1)
        level_2=new_df.to_dict('records')

        new_df=new_dbobj.read_table(query2)
        level_3=new_df.to_dict('records')

        main_dict={}
        main_dict["message"]= "Data Retrived successfully"

        main_dict["level_1"]=level_1
        main_dict["level_2"]=level_2
        main_dict["level_3"]=level_3
        new_dbobj.close_conn_eng()
        
        return main_dict
        
    except Exception as e:
        logger.error("get-filter failed",exc_info=e)
        return None

@app.post("/v1/rfi-rfp/get-project-supplier-details")
async def get_project_supplier_details(project_id: int = Form(...)):
    try:
        resp_dict={}
        set_env_var()
        new_dbobj=database(user_name , password , Database_Name , Server_Name)
        
        query_project=f'''select  * from {schema_name}.RFI_project
                where id = {project_id};'''

        project_df=new_dbobj.read_table(query_project)
        project_df = project_df.fillna(value='null')
        project_details=project_df.to_dict('records')
        resp_dict['project_details']=project_details
        
        query=f'''select  * from {schema_name}.RFI_suppli_project dt
        join {schema_name}.Vendors_Data vd on dt.supplier_id=vd.id
        where project_id = {project_id}'''

        new_df=new_dbobj.read_table(query)
        new_df = new_df.fillna(value='null')
        supplier_details=new_df.to_dict('records')
        resp_dict['supplier_details']=supplier_details
        resp_dict["message"]= "Data Retrived successfully"

        new_dbobj.close_conn_eng()
        return resp_dict
    except:
        return None
    

@app.post("/v1/competitor-analysis-dashboard/get-filters")
async def competitor_analysis_dashboard_get_filters():
    try:
        filters_dict={}
        set_env_var()
        new_dbobj=database(user_name , password , Database_Name , Server_Name)
        
        country_query=f'''SELECT DISTINCT
                        Country_of_Origin
                      FROM 
                        {schema_name}.app_faurecia_data
                      ORDER BY 
                        Country_of_Origin  
                        ;'''
        country_df=new_dbobj.read_table(country_query)
        country_filters=country_df.to_dict('records')
        filters_dict['country_filters']=country_filters

        importer_query=f'''SELECT DISTINCT
                            Importer
                          FROM 
                            {schema_name}.app_faurecia_data
                          ORDER BY 
                            Importer'''

        importer_df=new_dbobj.read_table(importer_query)
        importer_filters=importer_df.to_dict('records')
        filters_dict['importer_filters']=importer_filters

        hs_query=f'''SELECT DISTINCT
                            HS
                          FROM 
                            {schema_name}.app_faurecia_data
                          ORDER BY 
                            HS  '''
        hs_df=new_dbobj.read_table(hs_query)
        hs_filters=hs_df.to_dict('records')
        filters_dict['hs_filters']=hs_filters


        provider_query= f'''SELECT DISTINCT
                            Provider
                          FROM 
                            {schema_name}.app_faurecia_data
                          ORDER BY 
                            Provider  '''

        provider_df=new_dbobj.read_table(provider_query)
        provider_filters=provider_df.to_dict('records')
        filters_dict['provider_filters']=provider_filters

        support_type_query=f'''SELECT DISTINCT
                            Supplier_type
                          FROM 
                            {schema_name}.app_faurecia_data'''

        support_type_df=new_dbobj.read_table(support_type_query)
        support_type_filters=support_type_df.to_dict('records')
        filters_dict['supporter_type_filters']=support_type_filters

        new_dbobj.close_conn_eng()
        return filters_dict
    except:
        return None

@app.post("/v1/competitor-analysis-dashboard/all-competitors-data")
async def competitor_analysis_dashboard_all_competitors_data(Supplier_Type :str, Country_of_Origin: str, HS:str, Importer:str):
    try:
        set_env_var()
        Supplier_Type=Supplier_Type[1:-1]
        Country_of_Origin=Country_of_Origin[1:-1]
        HS=HS[1:-1]
        Importer=Importer[1:-1]
        new_dbobj=database(user_name , password , Database_Name , Server_Name)
        competitor_analysis_query=f'''SELECT
                            Provider_State_City,
                            Port,
                            Importer_City,
                            Naics_Code,
                            Unit_Value_USD,
                            Importer,
                            Commercial_Unit,
                            Port_ID,
                            Importer_State,
                            Importer_Address,
                            HS_Description,
                            Provider_State_Declared,
                            Naics_Classification,
                            Port_State,
                            Weight,
                            [Date],
                            Provider,
                            Provider_Address,
                            Product_HS,
                            Provider_Declared,
                            Transport_Method,
                            Customs_Value_USD,
                            Commercial_Quantity,
                            row_id,
                            Country_of_Origin,
                            HS,
                            HS_Desc,
                            [Time],
                            Supplier_Type
                          FROM 
                            {schema_name}.app_faurecia_data'''

        flag=any((Supplier_Type,Country_of_Origin,HS,Importer))

        already_condition_exist=0

        if flag:
            competitor_analysis_query+=' where'        
            if Supplier_Type:
                if already_condition_exist==1:
                    competitor_analysis_query+=' and '            

                competitor_analysis_query+=' Supplier_Type in ('+str(Supplier_Type)+')'
                already_condition_exist=1

            if Country_of_Origin:
                if already_condition_exist==1:
                    competitor_analysis_query+=' and '            

                competitor_analysis_query+=' Country_of_Origin in ('+str(Country_of_Origin)+')'
                already_condition_exist=1

            if HS:
                if already_condition_exist==1:
                    competitor_analysis_query+=' and '            

                competitor_analysis_query+=' HS in ('+str(HS)+')'
                already_condition_exist=1

            if Importer:
                if already_condition_exist==1:
                    competitor_analysis_query+=' and '            

                competitor_analysis_query+=' Importer in ('+str(Importer)+')'
                already_condition_exist=1

        competitor_analysis_query+=';'


        competitor_df=new_dbobj.read_table(competitor_analysis_query)

        all_competitors_data=competitor_df.to_dict('records')    
        new_dbobj.close_conn_eng()

        return all_competitors_data
    except:
        return None





@app.post("/v1/competitor-analysis-dashboard/competitors-weight-distribution")
async def competitor_analysis_dashboard_all_competitors_data(Supplier_Type :str, Country_of_Origin: str, HS:str, Importer:str):
    try:
        return_dict={}
        set_env_var()
        Supplier_Type=Supplier_Type[1:-1]
        Country_of_Origin=Country_of_Origin[1:-1]
        HS=HS[1:-1]
        Importer=Importer[1:-1]
        new_dbobj=database(user_name , password , Database_Name , Server_Name)

        weights_subquery=f''' select
                        SUM(Weight)
                      from
                        {schema_name}.app_faurecia_data'''


        flag=any((Supplier_Type,Country_of_Origin,HS,Importer))

        already_condition_exist=0
        already_condition_exist_outer=0

        if flag:
            weights_subquery+=' where'        
            if Supplier_Type:
                if already_condition_exist==1:
                    weights_subquery+=' and '            

                weights_subquery+=' Supplier_Type in ('+str(Supplier_Type)+')'
                already_condition_exist=1

            if Country_of_Origin:
                if already_condition_exist==1:
                    weights_subquery+=' and '            

                weights_subquery+=' Country_of_Origin in ('+str(Country_of_Origin)+')'
                already_condition_exist=1

            if HS:
                if already_condition_exist==1:
                    weights_subquery+=' and '            

                weights_subquery+=' HS in ('+str(HS)+')'
                already_condition_exist=1

            if Importer:
                if already_condition_exist==1:
                    weights_subquery+=' and '            

                weights_subquery+=' Importer in ('+str(Importer)+')'
                already_condition_exist=1

        countriwise_weights=f'''SELECT
                                          CAST(100 * (SUM(Weight)/(
                                            {weights_subquery})) AS DECIMAL(12,4)) As Weight_Percentage, 
                                            Country_of_Origin,
                                            COUNT(Provider) AS No_Of_Suppliers
                                          FROM
                                            {schema_name}.app_faurecia_data'''

        if flag:
            countriwise_weights+=' where'        
            if Supplier_Type:
                if already_condition_exist_outer==1:
                    countriwise_weights+=' and '            

                countriwise_weights+=' Supplier_Type in ('+str(Supplier_Type)+')'
                already_condition_exist_outer=1

            if Country_of_Origin:
                if already_condition_exist_outer==1:
                    countriwise_weights+=' and '            

                countriwise_weights+=' Country_of_Origin in ('+str(Country_of_Origin)+')'
                already_condition_exist_outer=1

            if HS:
                if already_condition_exist_outer==1:
                    countriwise_weights+=' and '            

                countriwise_weights+=' HS in ('+str(HS)+')'
                already_condition_exist_outer=1

            if Importer:
                if already_condition_exist_outer==1:
                    countriwise_weights+=' and '            

                countriwise_weights+=' Importer in ('+str(Importer)+')'
                already_condition_exist_outer=1

        countriwise_weights+=''' GROUP BY
              Country_of_Origin
            ORDER BY 
              Weight_Percentage DESC ;'''

        countriwise_weight_df=new_dbobj.read_table(countriwise_weights)

        countriwise_weight_data=countriwise_weight_df.to_dict('records')    

        return_dict['Country_Weight_Distribution']=countriwise_weight_data

        weights_subquery=f''' select
                        SUM(Weight)
                      from
                        {schema_name}.app_faurecia_data'''

        flag=any((Supplier_Type,Country_of_Origin,HS,Importer))

        already_condition_exist=0
        already_condition_exist_outer=0

        if flag:
            weights_subquery+=' where'        
            if Supplier_Type:
                if already_condition_exist==1:
                    weights_subquery+=' and '            

                weights_subquery+=' Supplier_Type in ('+str(Supplier_Type)+')'
                already_condition_exist=1

            if Country_of_Origin:
                if already_condition_exist==1:
                    weights_subquery+=' and '            

                weights_subquery+=' Country_of_Origin in ('+str(Country_of_Origin)+')'
                already_condition_exist=1

            if HS:
                if already_condition_exist==1:
                    weights_subquery+=' and '            

                weights_subquery+=' HS in ('+str(HS)+')'
                already_condition_exist=1

            if Importer:
                if already_condition_exist==1:
                    weights_subquery+=' and '            

                weights_subquery+=' Importer in ('+str(Importer)+')'
                already_condition_exist=1

        countriwise_weights=f'''SELECT
                                          CAST(100 * (SUM(Weight)/(
                                            {weights_subquery})) AS DECIMAL(12,4)) As Weight_Percentage, 
                                            Country_of_Origin,
                                            COUNT(Provider) AS No_Of_Suppliers
                                          FROM
                                            {schema_name}.app_faurecia_data'''

        if flag:
            countriwise_weights+=' where'        
            if Supplier_Type:
                if already_condition_exist_outer==1:
                    countriwise_weights+=' and '            

                countriwise_weights+=' Supplier_Type in ('+str(Supplier_Type)+')'
                already_condition_exist_outer=1

            if Country_of_Origin:
                if already_condition_exist_outer==1:
                    countriwise_weights+=' and '            

                countriwise_weights+=' Country_of_Origin in ('+str(Country_of_Origin)+')'
                already_condition_exist_outer=1

            if HS:
                if already_condition_exist_outer==1:
                    countriwise_weights+=' and '            

                countriwise_weights+=' HS in ('+str(HS)+')'
                already_condition_exist_outer=1

            if Importer:
                if already_condition_exist_outer==1:
                    countriwise_weights+=' and '            

                countriwise_weights+=' Importer in ('+str(Importer)+')'
                already_condition_exist_outer=1


        countriwise_weights+=''' GROUP BY
              Country_of_Origin,Provider
            ORDER BY 
              Weight_Percentage DESC ;'''

        countriwise_provider_weight_df=new_dbobj.read_table(countriwise_weights)

        countriwise_provider_weight_data=countriwise_provider_weight_df.to_dict('records')    

        return_dict['Provider_Weight_Distribution']=countriwise_provider_weight_data

        return return_dict
    except:
        return None

@app.get("/v1/competitor-analysis-dashboard/get-top-regions-by-company")
async def competitor_analysis_dashboard_get_top_regions_by_company():
    try:
        set_env_var()
        return_list=[]
        new_dbobj=database(user_name , password , Database_Name , Server_Name)
        distinct_company_group=get_distinct_company_group_names(new_dbobj,schema_name)

        for each_comp_group in distinct_company_group:
            return_list.append(get_top_regions_by_company(new_dbobj,schema_name,each_comp_group))
        return return_list
    except:
        return None

@app.get("/v1/competitor-analysis-dashboard/HS-code-analysis-by-country")
async def HS_code_analysis_by_country():
    try:
        set_env_var()
        new_dbobj=database(user_name , password , Database_Name , Server_Name)

        analysis_by_country_query=f'''SELECT 
        --TOP 5
        afd.country_of_origin,
        afd.hs,
        SUM(afd.weight) as weight
      FROM
        {schema_name}.app_faurecia_data afd
      WHERE 
        afd.hs != 0
      GROUP BY
        afd.country_of_origin,
        afd.hs
      ORDER BY
        SUM(afd.weight) DESC;'''
        
        analysis_by_country_df=new_dbobj.read_table(analysis_by_country_query)
                
        unique_country=analysis_by_country_df['country_of_origin'].unique()

        main_dict={}
        for each_country in unique_country:
            country_df=analysis_by_country_df.copy()
            country_df=country_df[country_df['country_of_origin']==each_country]
            country_df.reset_index(inplace = True, drop = True)
            hs_data_list=[]
            inner_dict={}
            outer_list=[]

            for itr in range(len(country_df)):
                most_inner_dict={}
                most_inner_dict['hs']=int(country_df['hs'][itr])
                most_inner_dict['weight']=country_df['weight'][itr]
                hs_data_list.append(most_inner_dict)
            inner_dict['country_of_origin']=each_country
            inner_dict['hs_data']=hs_data_list
            outer_list.append(inner_dict)
            main_dict[each_country]=outer_list

        return main_dict
    except:
        return None
        

@app.post("/v1/rfi-rfp/search-suppliers/get-suppliers-information")
async def search_suppliers_get_suppliers_information_api_fun(supplier :str,category:str,region:str,level_1:str,level_2:str,level_3:str,text:str,project_id: int = Form(...)):
    try:

        set_env_var()
        new_dbobj=database(user_name , password , Database_Name , Server_Name)
        return search_suppliers_get_suppliers_information(new_dbobj,schema_name,supplier,category,region,level_1,level_2,level_3,text,project_id)
    except:
        return None