import yaml
import json
import time
from requests import post, get
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
def traning_model(config):
    
    endpoint = config['azure']['endpoint']
    apim_key = config['azure']['apim_key']
    source = config['azure']['source']
    prefix = config['azure']['prefix']
    
    log_util.logger.debug("Azure Config: enpoint: {} \n, apim_key: {} \n, source: {} \n".format(endpoint, apim_key, source))

    post_url = endpoint + r"/formrecognizer/v2.0-preview/custom/models"
    includeSubFolders = False
    useLabelFile = True
    #
    headers = {
        # Request headers
        'Content-Type': 'application/json',
        'Ocp-Apim-Subscription-Key': apim_key,
    }
    
    body =  {
        "source": source,
        "sourceFilter": {
            "prefix": prefix,
            "includeSubFolders": includeSubFolders
        },
        "useLabelFile": useLabelFile
    }
    try:
        resp = post(url = post_url, json = body, headers = headers)
        if resp.status_code != 201:
            print("POST model failed (%s):\n%s" % (resp.status_code, json.dumps(resp.json())))
            #quit()
        print("POST model succeeded:\n%s" % resp.headers)
        model_loc = resp.headers["location"]
        return get_model(config, model_loc)
    except Exception as e:
        print("POST model failed:\n%s" % str(e))

@log_util.debug
def get_model(config, model_loc):
    
    apim_key = config['azure']['apim_key']
    
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


if __name__ == '__main__':
    
    config = init_config('config.yaml')
    model_id = traning_model(config)
    print('Model ID : {}'.format(model_id))
    
    
    

    