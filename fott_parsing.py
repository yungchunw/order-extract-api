import json
import time

import yaml
from azure.storage.blob import BlobServiceClient
from requests import get, post

from util_lib import log_util


@log_util.debug
def init_config(yaml_file_path):
    """
    init config from local yaml file

    Arguments:
        yaml_file_path {[str]} -- yaml file path string

    Returns:
        [config] -- configuration obj 
    """
    with open(yaml_file_path, "r") as stream:
        config = yaml.load(stream, Loader=yaml.FullLoader)
    return config


@log_util.debug
def get_modelid(config, prefix_id):
    """[summary]
    
    Arguments:
        config {[dict]} -- configuration obj 
    
    Returns:
        [dcit] -- fott file content 
    """

    container_name = config['azure_blob']['source']
    conn_str = config['azure_blob']['conn_str']

    # Create the BlobServiceClient that is used to call the Blob service for the storage account
    client = BlobServiceClient.from_connection_string(conn_str=conn_str)

    try:
        destination_file_name = "{}_training.fott".format(prefix_id)

        # Create a blob client using the local file name as the name for the blob
        blob_client = client.get_blob_client(container=container_name,
                                            blob=destination_file_name)
    
        blob_data = blob_client.download_blob().readall()

        data = json.loads(blob_data)

        return data["trainRecord"]["modelInfo"]["modelId"]
    
    except Exception as e:
        log_util.logger.error(e)

