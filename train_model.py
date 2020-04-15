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
        config = yaml.load(stream, Loader=yaml.FullLoader)
    return config

@log_util.debug
def traning_model(config):
    
    endpoint = config['azure']['endpoint']
    apim_key = config['azure']['apim_key']
    source = config['azure']['source']
    prefix = config['azure']['prefix']
    service_url = config['azure']['service_url']
    
    log_util.logger.debug("Azure Config: enpoint: {} \n, apim_key: {} \n, source: {} \n".format(endpoint, apim_key, source))

    post_url = endpoint + service_url
    include_sub_folders = False
    use_label_file = True
    
    # Request headers
    headers = {
        'Content-Type': 'application/json',
        'Ocp-Apim-Subscription-Key': apim_key}
    
    body =  {"source": source,
             "sourceFilter": {
                 "prefix": prefix,
                 "includeSubFolders": include_sub_folders},
             "useLabelFile": use_label_file}
    try:
        resp = post(url = post_url, json = body, headers = headers)
        if resp.status_code != 201:
            print("POST model failed (%s):\n%s" % (resp.status_code, json.dumps(resp.json())))
            #quit()
        print("POST model succeeded:\n%s" % resp.headers)
        location = resp.headers["location"]
        return location
    
    except Exception as e:
        print("POST model failed:\n%s" % str(e))

def main():
    
    config = init_config('config.yaml')
    model_loc = traning_model(config)
    print('Model Loc : {}'.format(model_loc))
    
if __name__ == '__main__':
    main()

    
    
    

    