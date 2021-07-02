import logging
import azure.functions as func
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, DataFormat, ReportLevel, ReportMethod
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential
import pandas as pd
# import tables
import io
import os


def createTableAndIngest(KUSTO_CLIENT, blobclient, commands, SAS_TOKEN, ACCOUNT_NAME, KUSTO_DATABASE):   
    print("Inside createTableAndIngest") 
    containerclient=blobclient.get_container_client('transformed')
    for key,value in commands.items():
        print("Inside loop") 
        RESPONSE = KUSTO_CLIENT.execute_mgmt(KUSTO_DATABASE, str(value))
        dataframe_from_result_table(RESPONSE.primary_results[0])
        # logging.info(dataframe_from_result_table(RESPONSE.primary_results[0]))
        for blobs in containerclient.list_blobs():
            if key.lower() in blobs['name']:
                if ".csv" in blobs['name']:
                    print(blobs['name'])
                    FILE_PATH = blobs['name']
                    break
        print("FILE_PATH:", FILE_PATH)
        BLOB_PATH = "https://" + ACCOUNT_NAME + ".blob.core.windows.net/transformed" + "/" + FILE_PATH + SAS_TOKEN
        print("BLOB_PATH: ", BLOB_PATH)

        # if key == "Demographics": 
        #     query = f".ingest into table Demographics(h'{BLOB_PATH}') with (ignoreFirstRecord=true);"
        #     logging.info("Demographics", query)
        # elif key == "Customers":
        #     query = f".ingest into table Customers(h'{BLOB_PATH}') with (ignoreFirstRecord=true);"
        #     logging.info("Customers", query)
        # elif key == "Orders":
        #     query = f".ingest into table Orders(h'{BLOB_PATH}') with (ignoreFirstRecord=true);"
        #     logging.info("Orders", query)
        # elif key == "Products":
        #     query = f".ingest into table Products(h'{BLOB_PATH}')with (ignoreFirstRecord=true);"
        #     logging.info("Products", query)
        if "demographics" in BLOB_PATH: 
            query = f".ingest into table Demographics(h'{BLOB_PATH}') with (ignoreFirstRecord=true);"
            print("Demographics", query)
        elif "customers" in BLOB_PATH:
            query = f".ingest into table Customers(h'{BLOB_PATH}') with (ignoreFirstRecord=true);"
            print("Customers", query)
        elif "orders" in BLOB_PATH:
            query = f".ingest into table Orders(h'{BLOB_PATH}') with (ignoreFirstRecord=true);"
            print("Orders", query)
        elif "products" in BLOB_PATH:
            query = f".ingest into table Products(h'{BLOB_PATH}') with (ignoreFirstRecord=true);"
            print("Products", query)
        query_fun(KUSTO_CLIENT, KUSTO_DATABASE, query)
        print("Ingest finish")
        

def query_fun(KUSTO_CLIENT, KUSTO_DATABASE, query):
    RESPONSE1 = KUSTO_CLIENT.execute_mgmt(KUSTO_DATABASE, query)
    dataframe_from_result_table(RESPONSE1.primary_results[0])


def executeKustoQueries(blobclient, KUSTO_CLIENT, query, CONTAINER, KUSTO_DATABASE):
    logging.info("Inside executeKustoQueries")
    for key,value in query.items():
        response = KUSTO_CLIENT.execute(KUSTO_DATABASE, str(value))
        df = dataframe_from_result_table(response.primary_results[0])
        output = df.to_csv (index_label="idx", encoding = "utf-8")
        blob_client = blobclient.get_blob_client(
            container=f"publish/{key}", blob=str(key)+".csv"
        )
        blob_client.upload_blob(output, blob_type="BlockBlob")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Process Started....")

    try:
        req_body = req.get_json() 
        logging.info(req_body)
    except Exception as e:
        func.HttpResponse(str(e), status_code=400)

    env = os.environ
    AAD_TENANT_ID = env['TENANT_ID']
    KUSTO_URI = env["KUSTO_URI"]
    # KUSTO_INGEST_URI = env["INGEST_URI"]
    KUSTO_DATABASE = env["KUSTO_DB"]
    CONTAINER = env["STORAGE_CONTAINER"]
    ACCOUNT_NAME = env["SINK_STORAGE_ACC"]
    SAS_TOKEN =  req_body['sinkStorageSasToken']
    STORAGE_ACC_KEY = env['SINK_STORAGE_ACC_KEY']
    CLIENT_ID = env['CLIENT_ID']
    CLIENT_SECRET = env['CLIENT_SECRET']


    FILE_SIZE = 64158321
    storageconnectionstring = f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT_NAME};AccountKey={STORAGE_ACC_KEY};EndpointSuffix=core.windows.net"

    #KUSTO CLIENT
    # KCSB_INGEST = KustoConnectionStringBuilder.with_aad_application_key_authentication(KUSTO_INGEST_URI, CLIENT_ID, CLIENT_SECRET, AAD_TENANT_ID)
    KCSB_DATA = KustoConnectionStringBuilder.with_aad_application_key_authentication(KUSTO_URI, CLIENT_ID, CLIENT_SECRET, AAD_TENANT_ID)
    KUSTO_CLIENT = KustoClient(KCSB_DATA)

    #KUSTO CREATE TABLE QUERIES
    commands = {'Customers':'.create table Customers(customer_id:string,first_name:string,last_name:string,region:string,state:string,cbgid:decimal,marital_status:string,education_level:string,age:int,gender:string);','Orders':'.create table Orders(customer_id:string,sku:string,order_date:string,product_quantity:string,amount_spent:decimal,latitude:string,longitude:string,payment_mode:string);','Products':'.create table Products(sku:string,product_category:string,link:string,company:string,price:decimal,release_date:string);','Demographics':'.create table Demographics(cbgid:int,state:string,population:int,population_density:decimal,households:int,middle_aged_people:int,household_income:decimal,bachelors_degrees:int,families_with_children:int,children_under_5:int,owner_occupied:int,marriedcouple_family:int);'}

    #EXPORT AND TRANSFORMATION KUSTO QUERY
    query = {'gender_product_category_count_result':'set notruncation; Products | join (Orders) on $left.sku == $right.sku| join (Customers) on $left.customer_id== $right.customer_id | summarize Amount_Spent = sum(amount_spent),gender_product_category_count = count() by gender, product_category','order_distribution_by_day_of_week_result':'set notruncation; Orders|extend updated_order_date=dayofweek(todatetime(order_date))| extend day_of_week= format_timespan(updated_order_date,"d")|summarize order_distribution=count()by day_of_week','payment_type_result':'set notruncation; Orders |summarize amount_spent = sum(amount_spent),number_of_orders = count() by payment_mode', 'state_wise_order_distribution_result':'set notruncation; Orders | summarize number_of_orders = count() by customer_id| join kind=inner Customers on $left.customer_id == $right.customer_id| summarize number_of_orders_by_state = sum(number_of_orders)by state, gender','gender_orders_by_month_result':'set notruncation; Orders | extend Month = getmonth(todatetime(order_date)), Year = getyear(todatetime(order_date))| join (Customers | where gender == "M") on $left.customer_id == $right.customer_id | summarize Male_orders = count() by Year,Month | join ( Orders | extend Month = getmonth(todatetime(order_date)), Year = getyear(todatetime(order_date)) | join (Customers | where gender == "F")on $left.customer_id == $right.customer_id | summarize Female_orders = count() by Year,Month ) on Year,Month | join ( Orders| extend Month = getmonth(todatetime(order_date)), Year = getyear(todatetime(order_date))| join (Customers)on $left.customer_id == $right.customer_id| summarize Total_orders = count() by Year,Month )on Year,Month| project  Male_orders,Female_orders,Total_orders','house_hold_income_distribution_result':'set notruncation; Demographics| extend household_income_category = case(household_income < 50000,"Under 50K",household_income >= 50000 and  household_income < 100000,"50k-100k",household_income >= 100000 and household_income < 200000,"100k-200k",household_income >= 200000,"Above 200k","NA")|where isnotnull(household_income)|summarize family_count=count() by household_income_category', 'loyal_customer_result':'set notruncation; Orders |summarize by customer_id,order_date |summarize  first_order_date= min(order_date),latest_order_Date = max(order_date)  by customer_id| join (Customers) on $left.customer_id == $right.customer_id|extend  customer_name = strcat(first_name,last_name)|extend DaysAsCustomer =datetime_diff("day",todatetime(latest_order_Date),todatetime(first_order_date))|project first_order_date,latest_order_Date,customer_name,DaysAsCustomer|summarize by customer_name,first_order_date,latest_order_Date', 'big_spenders_result':'set notruncation; Orders | summarize total_amt_spent = sum(amount_spent), avg_amt_spent = avg(amount_spent),number_of_orders = count() by customer_id|join (Customers |extend customer_name = strcat(first_name," ",last_name)) on $left.customer_id == $right.customer_id |project customer_name,total_amt_spent,avg_amt_spent,number_of_orders','age_wise_exp_dist_result_2':'set notruncation; Customers | extend customer_name = strcat(first_name,"",last_name)| extend age_grp_category = case( age >= 18 and age <30, "18-29",  age >= 30 and age <50 , "30-49", age >=50 and age <65, "50-64", "65-older")| summarize by gender,customer_id,customer_name, age_grp_category | join (Orders) on $left.customer_id == $right.customer_id| summarize expenditure = sum(amount_spent)  by customer_name,gender, age_grp_category'}

    #BLOB CLIENT
    blobclient = BlobServiceClient.from_connection_string(storageconnectionstring)

    try:
        
        logging.info(req_body)
        if req_body['name'] == "adhocanalysis":
            createTableAndIngest(KUSTO_CLIENT, blobclient, commands, SAS_TOKEN, ACCOUNT_NAME, KUSTO_DATABASE)
            logging.info("createTableAndIngest completed......")
            executeKustoQueries(blobclient,KUSTO_CLIENT,query,CONTAINER,KUSTO_DATABASE)
        return func.HttpResponse("Process Completed...", status_code=200)
    except Exception as e:
        return func.HttpResponse(str(e), status_code=400)