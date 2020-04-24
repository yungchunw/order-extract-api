import os
import yaml
import time
import json
import argparse
import fott_parsing
from information_extract import extract_info # extract information of ocr_result
from merge_json import gen_merge_json # merge json
from requests import get, post
from util_lib import log_util
from azure.storage.blob import BlobServiceClient
from PyPDF2 import PdfFileWriter, PdfFileReader

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
        

def file_analyze(config, model_id, data_bytes):
    """To get result of form recognizer 
    
    Arguments:
        config {[dict]} -- configuration obj
        model_id {[str]} -- id of form recognizer model
        data_bytes {[byte]} -- pdf obj
    
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
def multi_recognizer(config, path, azure=True):
    """POST multiple pdf files from /data to API endpoint
    
    Arguments:
        config {[dict]} -- configuration obj
        path {[str]} -- folder path of testing data
    
    Keyword Arguments:
        azure {bool} -- output localtion,True for Azure_blob; Flase for local (default: {True})
    """
    
    container_name = config['azure_blob']['output']
    conn_str = config['azure_blob']['conn_str']
    
    # init data
    files_lst = []

    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.pdf' in file:
                files_lst.append(os.path.join(r, file))
    
    
    for _, fp in enumerate(files_lst, start=0):
        start_time = time.time()
        pdf_name = fp.split('/')[-1].split('.pdf')[0]
        prefix_id= fp.split('/')[-2]
        
        model_id = fott_parsing.get_modelid(config, prefix_id)
        print(prefix_id, model_id)
        inputpdf = PdfFileReader(open(fp, "rb"), strict=False)
        log_util.logger.debug("PDF file {} with {} of pages".format(pdf_name, inputpdf.numPages))
        
        json_list = []
        for i in range(inputpdf.numPages):
            output = PdfFileWriter()
            output.addPage(inputpdf.getPage(i))
            with open("tmp.pdf", "wb") as outputStream:
                output.write(outputStream)
                
            with open("tmp.pdf", "rb") as f:
                data_bytes = f.read()

            output_json_azure = file_analyze(config, model_id, data_bytes)
            ouput_json = extract_info(output_json_azure, pdf_name)
            json_list.append(ouput_json)

            file_name = './output/{}/{}_{}.json'.format(prefix_id,pdf_name,i)
            
            # saving meatadata
            if azure:
                data = json.dumps(output_json_azure, indent=4,ensure_ascii=False)
                sync_to_azure(container_name, conn_str, data, file_name)
                    
            else:
                continue
                with open(file_name, 'w') as outfile:   
                    json.dump(output_json_azure, outfile, indent=4, ensure_ascii=False)
                
                
            log_util.logger.debug("{} - {} seconds".format(pdf_name,(time.time() - start_time)))
        
        final_json = gen_merge_json(json_list)

        final_file_name = './Final_Json/{}/{}.json'.format(prefix_id,pdf_name)
        if azure:
            data = json.dumps(final_json, indent=4,ensure_ascii=False)
            sync_to_azure(container_name, conn_str, data, final_file_name)
                    
        else:
            if not os.path.exists('./Final_Json'):
                os.mkdir('./Final_Json')
            if not os.path.exists('./Final_Json/{}'.format(prefix_id)):
                os.mkdir('./Final_Json/{}'.format(prefix_id))
            with open(final_file_name, 'w') as outfile:   
                json.dump(final_json, outfile, indent=4, ensure_ascii=False)




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
        '-p', '--path', required=True, type=str, help='input path')
    parser.add_argument(
        '-azure','--azure_sync', required=False, default='True',type=str2bool,
         nargs='?',const=True, help='True for Azure_blob; Flase for local(default=True)')
    
    args = parser.parse_args()
    
    config = init_config('config.yaml')

    multi_recognizer(config, args.path, args.azure_sync)


if __name__ == '__main__':
    main()
    
    

    
    