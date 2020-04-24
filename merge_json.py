#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def gen_merge_json(json_list):
    """merge json
    
    Arguments:
        json_list -- a list of json

    Returns:
        merged json -- a json or list of json
    """
    print('merging json...')
    output_json = []
    for i, json_i in enumerate(json_list):
        if (i == 0): # first time
            output_json.append(json_i)
        else:
            for j ,json_j in enumerate(output_json):
                is_append = True
                if (json_i['header']['custPoNumber'] is not None) & (json_j['header']['custPoNumber'] is not None):
                    if json_i['header']['custPoNumber'] == json_j['header']['custPoNumber']:
                        output_json[j]['line'].extend(json_i['line'])
                        is_append = False
                        break
            if is_append:
                output_json.append(json_i)
    return output_json


if __name__ == '__main__':
    """

    undo

    """
    pass
