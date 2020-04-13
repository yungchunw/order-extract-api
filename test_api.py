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
def get_model(config):
    
    apim_key = config['azure']['apim_key']
    model_loc = config['azure']['model_loc']
    
    n_tries = config['request']['n_tries']
    n_try = config['request']['n_try']
    wait_sec = config['request']['wait_sec']
    max_wait_sec = config['request']['max_wait_sec']
    #
    headers = {
        # Request headers
        'Ocp-Apim-Subscription-Key': apim_key,
    }
    while n_try < n_tries:
        try:
            resp = get(url = model_loc, headers = headers)
            resp_json = resp.json()
            if resp.status_code != 200:
                print("GET model failed (%s):\n%s" % (resp.status_code, json.dumps(resp_json)))
                #quit()
                break
            model_status = resp_json["modelInfo"]["status"]
            if model_status == "ready":
                print("Training succeeded:\n%s" % json.dumps(resp_json))
                #quit()
                return resp_json["modelInfo"]["modelId"]
            if model_status == "invalid":
                print("Training failed. Model is invalid:\n%s" % json.dumps(resp_json))
                #quit()
                break
            # Training still running. Wait and retry.
            time.sleep(wait_sec)
            n_try += 1
            wait_sec = min(2*wait_sec, max_wait_sec)     
        except Exception as e:
            msg = "GET model failed:\n%s" % str(e)
            print(msg)
            #quit()
    print("Train operation did not complete within the allocated time.")
    return None

@log_util.debug
def test_form_recornizer():
    
    config = init_config('config.yaml')
    
    model_id = get_model(config)
    print(model_id)


test_form_recornizer()




    # Endpoint URL
    # endpoint = config['azure']['endpoint']
    # apim_key = config['azure']['apim_key']
    # source = config['azure']['source']
