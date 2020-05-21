#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from util_lib import log_util

@log_util.debug
def gen_merge_json(json_list, extras):
    """merge json
    
    Arguments:
        json_list -- a list of json

    Returns:
        merged json -- a json or list of json
    """
    log_util.logger.debug('merging json...',extra=extras)
    header_list = ['buyerName', 'supplierName', 'poDate', 'custPoNumber', 
                       'shipAddr', 'billAddr', 'deliverAddr', 'payCurrency',
                       'paymentTerm', 'tax', 'tradeTerm']
    output_json = []
    for i, json_i in enumerate(json_list):
        if (i == 0): # first time
            output_json.append(json_i)
        else:
            for j ,json_j in enumerate(output_json):
                is_append = True
                print(json_i['header']['custPoNumber'], json_j['header']['custPoNumber'])
                if (json_i['header']['custPoNumber'] != '') & (json_j['header']['custPoNumber'] != ''):
                    if json_i['header']['custPoNumber'] == json_j['header']['custPoNumber']:
                        # header
                        for col in header_list:
                            if (json_i['header'][col] == '') & (output_json[j]['header'][col] == ''):
                                output_json[j]['header'][col] = json_j['header'][col]
                            elif (json_j['header'][col] == '') & (output_json[j]['header'][col] == ''):
                                output_json[j]['header'][col] = json_i['header'][col]
                        # line
                        output_json[j]['line'].extend(json_i['line'])
                        is_append = False
                        break
                # 如果為null 一樣合併 (多頁無header的情況下)
                elif (json_i['header']['custPoNumber'] == '') or (json_j['header']['custPoNumber'] == ''):
                    # header
                    for col in header_list:
                        if (json_i['header'][col] == '') & (output_json[j]['header'][col] == ''):
                            output_json[j]['header'][col] = json_j['header'][col]
                        elif (json_j['header'][col] == '') & (output_json[j]['header'][col] == ''):
                            output_json[j]['header'][col] = json_i['header'][col]
                    # line
                    output_json[j]['line'].extend(json_i['line'])
                    is_append = False
                    break
            if is_append:
                output_json.append(json_i)
    return output_json