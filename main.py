import argparse
import json
import os
import random
import time
import uuid
import yaml
from azure.storage.blob import BlobServiceClient
from PyPDF2 import PdfFileReader, PdfFileWriter
from requests import get, post

import fott_parsing
from information_extract import (  # extract information of ocr_result
    extract_info, load_mapping_list)
from merge_json import gen_merge_json  # merge json
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

        log_util.logger.debug(
            'Syncing completed : {}'.format(destination_file_name))

    except Exception as e:
        log_util.logger.error(e)


@log_util.debug
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
        'Ocp-Apim-Subscription-Key': apim_key
    }

    try:
        random_time = random.randint(10, 20)
        print('random_time: {}s'.format(random_time))
        time.sleep(random_time)

        resp = post(url=post_url, data=data_bytes,
                    headers=headers, params=params)
        if resp.status_code != 202:
            print("POST analyze failed:\n%s" % json.dumps(resp.json()))
            # quit()
        print("POST analyze succeeded:\n%s" % resp.headers)
        operation_url = resp.headers["operation-location"]
        return get_result(config, operation_url), random_time
    except Exception as e:
        msg = "POST analyze failed:\n%s" % str(e)
        print(msg)
        log_util.logger.error(msg)
        # quit()
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
            resp = get(url=operation_url, headers={
                       "Ocp-Apim-Subscription-Key": apim_key})
            resp_json = resp.json()
            if resp.status_code != 200:
                print("GET analyze results failed:\n%s" %
                      json.dumps(resp_json))
                # quit()
                return None
            status = resp_json["status"]
            if status == "succeeded":
                #print("Analysis succeeded:\n%s" % json.dumps(resp_json))
                # quit()
                return resp_json
            if status == "failed":
                print("Analysis failed:\n%s" % json.dumps(resp_json))
                # quit()
                return None
            # Analysis still running. Wait and retry.
            time.sleep(wait_sec)
            n_try += 1
            wait_sec = min(2*wait_sec, max_wait_sec)
        except Exception as e:
            msg = "GET analyze results failed:\n%s" % str(e)
            print(msg)
            log_util.logger.error(msg)
            # quit()
            return None
    print("Analyze operation did not complete within the allocated time.")
    return None


@log_util.debug
def process(fp, prefix_id, azure=False):
    """POST multiple pdf files from /data to API endpoint

    Arguments:
        config {[dict]} -- configuration obj
        path {[str]} -- folder path of testing data

    Keyword Arguments:
        azure {bool} -- output localtion,True for Azure_blob; Flase for local (default: {True})
    """
    config = init_config('config.yaml')

    container_name = config['azure_blob']['output']
    conn_str = config['azure_blob']['conn_str']

    print('mapping list loading...')
    mapping_list_all = load_mapping_list()  # mapping list loading


    
    start_time = time.time()
    pdf_name = fp.split('/')[-1].split('.')[0]

    model_id = fott_parsing.get_modelid(config, prefix_id)

    print(prefix_id, model_id)
    inputpdf = PdfFileReader(open(fp, "rb"), strict=False)
    log_util.logger.debug("PDF file {} with {} of pages".format(
        pdf_name, inputpdf.numPages))

    json_list = []
    try:
        tmp_uuid = str(uuid.uuid4())
        tmp_path = './tmp/{}'.format(tmp_uuid)

        if not os.path.exists(tmp_path):
                os.mkdir(tmp_path)

        for i in range(inputpdf.numPages):
            output = PdfFileWriter()
            output.addPage(inputpdf.getPage(i))
            tmp_file = os.path.join(tmp_path,"tmp.pdf")
            with open(tmp_file, "wb") as outputStream:
                output.write(outputStream)

            with open(tmp_file, "rb") as f:
                data_bytes = f.read()

            output_json_azure, random_time = file_analyze(
                config, model_id, data_bytes)
            ouput_json = extract_info(
                output_json_azure, pdf_name, mapping_list_all)
            json_list.append(ouput_json)

            file_name = './output/{}_{}.json'.format(pdf_name, i)

            # saving meatadata
            if azure:
                data = json.dumps(output_json_azure,
                                    indent=4, ensure_ascii=False)
                sync_to_azure(container_name, conn_str, data, file_name)

            else:
                if not os.path.exists('./output'):
                    os.mkdir('./output')

                with open(file_name, 'w') as outfile:
                    json.dump(output_json_azure, outfile,
                                indent=4, ensure_ascii=False)

            log_util.logger.debug(
                "{} - {} seconds".format(pdf_name, (time.time() - start_time - random_time)))

        final_json = gen_merge_json(json_list)

        final_file_name = './Final_Json/{}.json'.format(pdf_name)
        if azure:
            data = json.dumps(final_json, indent=4, ensure_ascii=False)
            sync_to_azure(container_name, conn_str, data, final_file_name)

        else:
            if not os.path.exists('./Final_Json'):
                os.mkdir('./Final_Json')

            with open(final_file_name, 'w') as outfile:
                json.dump(final_json, outfile,
                            indent=4, ensure_ascii=False)
            return final_json

    except Exception as e:
        print(e)
        log_util.logger.error(e)
        log_util.logger.debug("{} - 999 seconds".format(pdf_name))
        return None