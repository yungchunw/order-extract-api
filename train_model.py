import yaml
import json
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
        get_url = resp.headers["location"]
        return get_url
    except Exception as e:
        print("POST model failed:\n%s" % str(e))

if __name__ == '__main__':

    config = init_config('config.yaml')
    
    url = traning_model(config)
    print(url)

    