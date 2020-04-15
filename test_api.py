import os
import yaml
import time
import json
import argparse
from requests import get, post
from util_lib import log_util
from azure.storage.blob import BlobServiceClient


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


def sync_to_azure(container_name, conn_str, data, file_name):
    """sync to azure blob
    
    Arguments:
        container_name {[str]} -- Azure container(blob) name
        conn_str {[str]} -- Connected string of Azure blob
        data {[byte]} -- json file
        file_name {[str]} -- destination of file name
    """

    try:
        # Create the BlobServiceClient that is used to call the Blob service for the storage account
        client = BlobServiceClient.from_connection_string(conn_str=conn_str)

        destination_file_name = file_name

        # Create a blob client using the local file name as the name for the blob
        blob_client = client.get_blob_client(container=container_name,
                                                blob=destination_file_name)

        # Upload the created file
        blob_client.upload_blob(data, overwrite=True)

        log_util.logger.debug('Syncing completed : {}'.format(destination_file_name))
        
    except Exception as e:
        log_util.logger.debug(e)
        

def file_analyze(config, model_id, file_path):
    """To get result of form recognizer 
    
    Arguments:
        config {[dict]} -- configuration obj
        model_id {[str]} -- id of form recognizer model
        file_path {[str]} -- testing file
    
    Returns:
        [str] -- operation-location
    """
    apim_key = config['azure']['apim_key']
    service_url = config['azure']['service_url']
    endpoint = config['azure']['endpoint']
  
    post_url = endpoint + service_url + model_id + "/analyze"
    params = {
        "includeTextDetails": True
    }
    headers = {
        # Request headers
        'Content-Type': 'application/pdf',
        'Ocp-Apim-Subscription-Key': apim_key,
    }
    with open(file_path, "rb") as f:
        data_bytes = f.read()
    
    try:
        resp = post(url = post_url, data = data_bytes, headers = headers, params = params)
        if resp.status_code != 202:
            print("POST analyze failed:\n%s" % json.dumps(resp.json()))
            #quit()
        print("POST analyze succeeded:\n%s" % resp.headers)
        operation_url = resp.headers["operation-location"]
        return get_result(config, operation_url)
    except Exception as e:
        print("POST analyze failed:\n%s" % str(e))
        #quit()
        return None
    
def get_result(config, operation_url):
    """To get result of form recognizer 
    
    Arguments:
        config {[dict]} -- configuration obj
        operation_url {[str]} -- operation url
    
    Returns:
        resp_json -- response of recognizer
    """
    
    
    apim_key = config['azure']['apim_key']
    n_tries = config['request']['n_tries']
    n_try = config['request']['n_try']
    wait_sec = config['request']['wait_sec']
    max_wait_sec = config['request']['max_wait_sec']
    while n_try < n_tries:
        try:
            resp = get(url = operation_url, headers = {"Ocp-Apim-Subscription-Key": apim_key})
            resp_json = resp.json()
            if resp.status_code != 200:
                print("GET analyze results failed:\n%s" % json.dumps(resp_json))
                #quit()
                return None
            status = resp_json["status"]
            if status == "succeeded":
                print("Analysis succeeded:\n%s" % json.dumps(resp_json))
                #quit()
                return resp_json
            if status == "failed":
                print("Analysis failed:\n%s" % json.dumps(resp_json))
                #quit()
                return None
            # Analysis still running. Wait and retry.
            time.sleep(wait_sec)
            n_try += 1
            wait_sec = min(2*wait_sec, max_wait_sec)     
        except Exception as e:
            msg = "GET analyze results failed:\n%s" % str(e)
            print(msg)
            #quit()
            return None
    print("Analyze operation did not complete within the allocated time.")
    return None

@log_util.debug
def multi_recognizer(config, model_id, path, azure=True):
    """POST multiple pdf files from /data to API endpoint
    
    Arguments:
        config {[dict]} -- configuration obj
        model_id {[str]} -- form recognizer model id
        path {[str]} -- folder path of testing data
    
    Keyword Arguments:
        azure {bool} -- output localtion,True for Azure_blob; Flase for local (default: {True})
    """
    
    container_name = config['azure_blob']['blob_name']
    conn_str = config['azure_blob']['conn_str']
    
    # init data
    files_lst = []

    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.pdf' in file:
                files_lst.append(os.path.join(r, file))
    
    if model_id != None:
        for _, fp in enumerate(files_lst, start=0):
            start_time = time.time()
            file_key = fp.split('/')[-1].split('.pdf')[0]
            output_json = file_analyze(config, model_id, fp)
            
            file_name = './output/{}.json'.format(file_key)
            
            if azure:
                data = json.dumps(output_json, indent=4,ensure_ascii=False)
                sync_to_azure(container_name, conn_str, data, file_name)
                    
            else:
                
                with open(file_name, 'w') as outfile:   
                    json.dump(output_json, outfile, indent=4, ensure_ascii=False)
                
                
            log_util.logger.debug("{} - {} seconds".format(file_key,(time.time() - start_time)))
            
    else:
        print('No model found!!')
   
def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def main():
    parser = argparse.ArgumentParser(description='PDF files analyze')
    parser.add_argument(
        '-i', '--id', required=True, type=str, help='model id')
    parser.add_argument(
        '-p', '--path', required=True, type=str, help='input path')
    parser.add_argument(
        '-azure','--azure_sync', required=False, default='True',type=str2bool,
         nargs='?',const=True, help='True for Azure_blob; Flase for local(default=True)')
    
    args = parser.parse_args()
    
    config = init_config('config.yaml')

    multi_recognizer(config, args.id, args.path, args.azure_sync)


if __name__ == '__main__':
    main()
    
    

    
    