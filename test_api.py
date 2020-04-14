import yaml
import time
import json
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
        config = yaml.load(stream)
    return config

@log_util.debug
def file_analyze(config, model_id, file_path):
    """[summary]
    
    Arguments:
        config {[type]} -- [description]
        model_id {[type]} -- [description]
        file_path {[type]} -- [description]
    
    Returns:
        [type] -- [description]
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
        get_url = resp.headers["operation-location"]
        return get_result(config, get_url)
    except Exception as e:
        print("POST analyze failed:\n%s" % str(e))
        #quit()
        return None
    
@log_util.debug 
def get_result(config, get_url):
    apim_key = config['azure']['apim_key']
    n_tries = config['request']['n_tries']
    n_try = config['request']['n_try']
    wait_sec = config['request']['wait_sec']
    max_wait_sec = config['request']['max_wait_sec']
    while n_try < n_tries:
        try:
            resp = get(url = get_url, headers = {"Ocp-Apim-Subscription-Key": apim_key})
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


if __name__ == '__main__':
    
    config = init_config('config.yaml')
    model_id =  'ba8e1c65-f7d0-45f3-9c9a-d58f9c6b9b30'
    file_path = '/Users/eltonwang/Downloads/Done/8774/testing/3921_62959_8774_3169.pdf'
    print('Analysis file...')
    if model_id != None:
        output_json = file_analyze(config, model_id, file_path)
        with open('output.json', 'w') as outfile:
           json.dump(output_json,outfile,indent=4,ensure_ascii=False)
    else:
        print('No model found!!')

    
    