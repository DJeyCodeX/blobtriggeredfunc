from typing import List
# from azure.cosmos import CosmosClient, PartitionKey
import logging
import json

import azure.functions as func

# connectionString = "8apypbQeZep7EZkDVNXzPdsA6JKCfyOTPAcHmcET9cR3SpHstqktXPoDxe0mg8PpLuE6heABdFCK9VH5c6US1g=="

# client = CosmosClient("https://cosmosdocdb.documents.azure.com", connectionString)

# database_name = 'demodb'
# database = client.create_database_if_not_exists(id=database_name)

# container_name = 'democontainer'
# container = database.create_container_if_not_exists(
#     id=container_name,
#     partition_key=PartitionKey(path="/subject"),
#     offer_throughput=400)

def main(events: List[func.EventHubEvent], cosmosdb: func.Out[func.Document]):
    for event in events:
        event_data = event.get_body()
        my_json = event_data.decode('utf8').replace("'", '"')
        event_data_json = json.loads(my_json)
        logging.info('EventHub trigger processed an event: %s',
                        event.get_body().decode('utf-8'))
        for i in event_data_json:
            cosmosdb.set(func.Document.from_dict(i))
