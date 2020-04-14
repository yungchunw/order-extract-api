#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json as js

file_path = 'output.json'

with open(file_path, 'r') as f:
    js_file = js.load(f)

result = js_file['analyzeResult']['documentResults'][0]['fields']

output_json = {'header':{}, 'line':[]}
# header
header_list = ['buyerName', 'supplierName', 'custPoNumber', 'poDate', \
               'shipAddr', 'billAddr', 'deliverAddr', 'payCurrency', \
                   'paymentTerm', 'tax', 'tradeTerm']
for col in header_list:
    if result[col] != None:
        output_json['header'][col] = result[col]['text']
    else:
        output_json['header'][col] = None

# line
count = 0
for key in result.keys():
    if 'lineNumber' in key:
        count += 1

line_list = ['lineNumber', 'custPartNo', 'sellingPrice', \
             'voQty', 'originalRequestDate']
for idx in range(1, count+1):
    line_info = {}
    for col in line_list:
        if result[col + '#' + str(idx)] != None:
            line_info[col] = result[col + '#' + str(idx)]['text']
        else:
            line_info[col] = None
    output_json['line'].append(line_info)

# check null of line
count = len(output_json['line'])
while count > 0:
    for idx, row in enumerate(output_json['line']):
        is_null = 0
        for val in row.values():
            if val == None:
                is_null += 1
        if is_null > 1: # if two cols are null, then del
            del output_json['line'][idx]
            break
    count -= 1

# output file

with open('result.json', 'w') as output_file:
    js.dump(output_json, output_file, ensure_ascii=False, indent=4)
    