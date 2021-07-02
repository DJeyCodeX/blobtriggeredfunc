import logging

import azure.functions as func
import os
import urllib3
from urllib.parse import urlencode
import pathlib
from azure.storage.blob import BlobClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory.models import *
import base64
import json
import requests

def aad_access_token(Tenant_Id, Client_Id, Client_Secret):
    print("inside access token")

    url = f"https://login.microsoftonline.com/{Tenant_Id}/oauth2/token"

    payload = f'grant_type=client_credentials&client_id={Client_Id}&client_secret={Client_Secret}' \
              f'&resource=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': 'fpc=ApHJ0WysE_dLpWvJTNFVMPQWkwsMAgAAANisQdgOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()


def azure_management_access_token(Tenant_Id, Client_Id, Client_Secret):
    print("inside azure token")

    url = f"https://login.microsoftonline.com/{Tenant_Id}/oauth2/token"

    payload = f'grant_type=client_credentials&client_id={Client_Id}&client_secret={Client_Secret}&resource=' \
              f'https%3A%2F%2Fmanagement.core.windows.net%2F'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': 'fpc=ApHJ0WysE_dLpWvJTNFVMPQWkwsMAgAAANisQdgOAAAA6u7QKwEAAACJrUHYDgAAAMbWMVsBAAAA_a9B2A4AAAA; '
                  'stsservicecookie=estsfd; x-ms-gateway-slice=estsfd'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return response.json()


def databricks_token(Workspace_Id, Management_Token, Subscription_Id, Resource_Group_Name, Workspace_Name, AAD_Token):

    url = f"https://{Workspace_Id}.azuredatabricks.net/api/2.0/token/create"

    payload = {}
    headers = {
        'X-Databricks-Azure-SP-Management-Token': f'{Management_Token}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'/subscriptions/{Subscription_Id}/resourceGroups/'
                                                    f'{Resource_Group_Name}/providers/Microsoft.Databricks/workspaces/'
                                                    f'{Workspace_Name}',
        'Authorization': f'Bearer {AAD_Token}'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return response.json()


def cluster_create(Workspace_Id, Management_Token, Subscription_Id, Resource_Group_Name, Workspace_Name, AAD_Token):
    url = f"https://{Workspace_Id}.azuredatabricks.net/api/2.0/clusters/create"

    payload = json.dumps({
        "cluster_name": "my-cluster",
        "spark_version": "6.4.x-scala2.11",
        "spark_conf": {},
        "node_type_id": "Standard_F4s",
        "autoscale": {
                "min_workers": 0,
                "max_workers": 2
        },
        "custom_tags": {},
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "autotermination_minutes": 120,
        "init_scripts": []
        })

    headers = {
        'X-Databricks-Azure-SP-Management-Token': f'{Management_Token}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'/subscriptions/{Subscription_Id}/resourceGroups/'
                                                    f'{Resource_Group_Name}/providers/Microsoft.Databricks/workspaces/'
                                                    f'{Workspace_Name}',
        'Authorization': f'Bearer {AAD_Token}'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return response.json()


def submit_notebook(Workspace_Username, Container, Workspace_Id, DB_Token, sourceStorageAccountName, sourceStorageAccountKey):
    dct = dict()
    lst = ["Notebook1.py", "Notebook2.py"]

    for notebook_name in lst:
        notebooknamewithoutext = notebook_name.split(".")
        notebooknamewithoutext = notebooknamewithoutext[0]
        path = "/Users/" + Workspace_Username + "/" + notebooknamewithoutext
        dct[notebooknamewithoutext + "path"] = path
        blob = BlobClient(account_url="https://" + sourceStorageAccountName + ".blob.core.windows.net", container_name=Container, blob_name= "notebooks/" + notebook_name, credential=sourceStorageAccountKey)

        # file_data = Blob_Client.get_blob_to_bytes(Container, 'notebooks/' + notebook_name).content
        file_data = blob.download_blob().readall()
        base64_two = base64.b64encode(file_data).decode('ascii')
        url = "https://" + Workspace_Id + ".azuredatabricks.net/api/2.0/workspace/import"
        data = {
            "content": base64_two,
            "path": path,
            "language": "PYTHON",
            "overwrite": True,
            "format": "SOURCE"
        }
        headers = {'Authorization': 'Bearer ' + DB_Token, 'Content-Type': 'application/json'}
        requests.post(url, data=json.dumps(data), headers=headers)

    return json.dumps(dct)


def cluster_list(Workspace_Id, Management_Token, Subscription_Id, Resource_Group_Name, Workspace_Name, AAD_Token):
    url = f"https://{Workspace_Id}.azuredatabricks.net/api/2.0/clusters/list"

    payload = {}

    headers = {
        'X-Databricks-Azure-SP-Management-Token': f'{Management_Token}',
        'X-Databricks-Azure-Workspace-Resource-Id': f'/subscriptions/{Subscription_Id}/resourceGroups/'
                                                    f'{Resource_Group_Name}/providers/Microsoft.Databricks/workspaces/'
                                                    f'{Workspace_Name}',
        'Authorization': f'Bearer {AAD_Token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    new_list = []

    for i in response.json().values():
        for n in i:
            if n["cluster_source"] == "UI":
                dict_copy = n.copy()
                new_list.append(dict_copy)
    return new_list


def get_token(http, tenant_id, client_id, client_secret):
    url = "https://login.microsoftonline.com/" + tenant_id + "/oauth2/token"
    resp = http.request('POST', 
                        url, 
                        fields={'grant_type': 'client_credentials',
                            'client_id': client_id,
                            'resource': 'https://management.azure.com/',
                            'client_secret': client_secret})
    resp_json = json.loads(resp.data.decode('utf-8'))
    return resp_json['access_token']


def db_datafactory_create(req_body, df_name, location, sourceStorageAccountName, sinkStorageAccountName, sourceStorageAccountKey, sourceContainerName, sinkContainerName, subscription, resource_grp, tenant_id, client_id, client_secret,  databricksUserName, databricksAccessToken, workspaceId, gremlinAccountName):
    http = urllib3.PoolManager()
    # file_name = "dataFactory"
    file_name = 'templates/dataFactory.json'
    blob = BlobClient(account_url="https://" + sourceStorageAccountName + ".blob.core.windows.net",
            container_name=sourceContainerName,
            blob_name=file_name,
            credential=sourceStorageAccountKey)
    print(sourceStorageAccountName)
    print(sourceStorageAccountKey)
    print(sourceContainerName)
    print(json.loads(blob.download_blob().readall()))
    template = json.loads(blob.download_blob().readall())
    print(template)
    # logging.info(template)

    if not template:
        return func.HttpResponse('Invalid template formed!', status_code=400)

    else:
        token = get_token(http, tenant_id, client_id, client_secret)
        parameters = req_body
        parameters['factoryName'] = df_name
        parameters['location']=location
        # parameters['sourceStorageAccountKey']=sourceStorageAccountKey
        parameters['sourceStorageAccountName']=sourceStorageAccountName
        parameters['sinkStorageAccountName']=sinkStorageAccountName
        parameters['sourceContainerName']=sourceContainerName
        parameters['sinkContainerName']=sinkContainerName
        parameters['databricksUserName']=databricksUserName
        parameters['databricksAccessToken']=databricksAccessToken
        parameters['gremlinAccountName']= gremlinAccountName
        parameters['workspaceId']=workspaceId
        parameters['clusterId']=req_body['clusterId']
        parameters.pop("name")
        parameters.pop("operationType")
        parameters.pop("workspaceId")
        logging.info("parameters")
        logging.info(parameters)
        parameters_json = dict()
        for key in parameters:
            parameters_json[key] = {'value': parameters[key]}
        
        url = "https://management.azure.com/subscriptions/" + subscription + "/resourcegroups/" + resource_grp + "/providers/Microsoft.Resources/deployments/datafactory?api-version=2019-05-01"
        headers = {'Authorization': 'Bearer ' + token,
                    'Content-Type': 'application/json',
                    'Host': 'management.azure.com'}
        data = {
                'properties': {
                    'mode': 'Incremental',
                    'template': template,
                    'parameters': parameters_json
                }
        }
        encoded_data = json.dumps(data).encode('utf-8')
        resp = http.request('PUT', url, body=encoded_data, headers=headers)
        result = json.loads(resp.data) 
        

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Process Started...')

    env = os.environ
    try:
        req_body = req.get_json() 
        logging.info(req_body)
    except Exception as e:
        func.HttpResponse(str(e), status_code=400)

    TENANT_ID = env['TENANT_ID']
    CLIENT_ID = env['CLIENT_ID']
    CLIENT_SECRET = env['CLIENT_SECRET']
    SUBSCRIPTION_ID = env['SUBSCRIPTION_ID']
    RESOURCE_GROUP_NAME = env['RESOURCE_GROUP_NAME']
    WORKSPACE_NAME = env['WORKSPACE_NAME']
    WORKSPACE_USERNAME = env['WORKSPACE_USERNAME']
    DATAFACTORY_NAME = env['DATAFACTORY_NAME']
    WORKSPACE_ID = req_body['workspaceId']
    LOCATION = env['LOCATION']
    SOURCE_STORAGE_ACC = env['SOURCE_STORAGE_ACC']
    SOURCE_STORAGE_ACC_KEY = env['SOURCE_STORAGE_ACC_KEY']
    # SINK_STORAGE_ACC_KEY = env['SINK_STORAGE_ACC_KEY']
    SINK_STORAGE_ACC = env['SINK_STORAGE_ACC']
    SOURCE_CONTAINER = env['SOURCE_CONTAINER']
    SINK_CONTAINER = env['SINK_CONTAINER']
    GREMLIN_ACCOUNT_NAME = env['GREMLIN_ACCOUNT_NAME']

    try:
        aad_token = aad_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
        print("aadtoken")
        print(aad_token['access_token'])

        azure_management_token = azure_management_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
        print("azure_token")
        print(azure_management_token['access_token'])

        db_token = databricks_token(WORKSPACE_ID, azure_management_token['access_token'], SUBSCRIPTION_ID,
                                        RESOURCE_GROUP_NAME, WORKSPACE_NAME, aad_token['access_token'])

        print(db_token['token_value'])

        if req_body['name'] == "databricksflow":
            if req_body['operationType'] == "submitnotebook":
                notebook_submit = submit_notebook(WORKSPACE_USERNAME, SOURCE_CONTAINER, WORKSPACE_ID, db_token['token_value'], SOURCE_STORAGE_ACC, SOURCE_STORAGE_ACC_KEY)

                logging.info(notebook_submit)
            elif req_body['operationType'] == 'createcluster':
                print("create cluster")
                create_cluster = cluster_create(WORKSPACE_ID, azure_management_token['access_token'], SUBSCRIPTION_ID, RESOURCE_GROUP_NAME, WORKSPACE_NAME, aad_token['access_token'])

                logging.info(create_cluster)
            elif req_body['operationType'] == 'listcluster':
                list_cluster = cluster_list(WORKSPACE_ID, azure_management_token['access_token'], SUBSCRIPTION_ID,
                                    RESOURCE_GROUP_NAME, WORKSPACE_NAME, aad_token['access_token'])

                logging.info(list_cluster)

            if req_body['clusterId'] != "":
                # db_token = databricks_token(WORKSPACE_ID, azure_management_token['access_token'], SUBSCRIPTION_ID,
                #                         RESOURCE_GROUP_NAME, WORKSPACE_NAME, aad_token['access_token'])

                # print(db_token['token_value'])

                db_datafactory_create(req_body, DATAFACTORY_NAME, LOCATION, SOURCE_STORAGE_ACC, SINK_STORAGE_ACC, SOURCE_STORAGE_ACC_KEY, SOURCE_CONTAINER, SINK_CONTAINER, SUBSCRIPTION_ID, RESOURCE_GROUP_NAME, TENANT_ID, CLIENT_ID, CLIENT_SECRET, WORKSPACE_USERNAME, db_token['token_value'], WORKSPACE_ID, GREMLIN_ACCOUNT_NAME)

        return func.HttpResponse("Process Completed...")
    except Exception as e:
        return func.HttpResponse(str(e), status_code=400)