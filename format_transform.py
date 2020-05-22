#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class gen_defined_output:
    def __init__(self):
        pass


    def azure_to_ori(azure_json_format):
        """
        Transform Azure Form Recognizer output json to defined json format,
        and set a rule to delete the line that has at least two columns empty.
        
        Arguments:
            azure_json_format  -- output json of azure
        
        Returns:
            output_json -- defined json format

        """
        result = azure_json_format['analyzeResult']['documentResults'][0]['fields']
        output_json = {'header': {}, 'line': []}
        # header
        header_list = ['buyerName', 'supplierName', 'custPoNumber', 'poDate',
                       'shipAddr', 'billAddr', 'deliverAddr', 'payCurrency',
                       'paymentTerm', 'tax', 'tradeTerm']
        for col in header_list:
            if col in result.keys(): # 確認此標籤是否存在
                if result[col] != None:
                    output_json['header'][col] = result[col]['text']
                else:
                    output_json['header'][col] = None
            else:
                output_json['header'][col] = None
        # line
        count = 0
        for key in result.keys():
            if 'lineNumber' in key:
                count += 1
        line_list = ['lineNumber', 'custPartNo', 'sellingPrice',
                     'voQty', 'originalRequestDate']
        for idx in range(1, count+1):
            line_info = {}
            for col in line_list:
                if (col + '#' + str(idx)) in result.keys(): # 確認此標籤是否存在
                    if result[col + '#' + str(idx)] != None:
                        line_info[col] = result[col + '#' + str(idx)]['text']
                    else:
                        line_info[col] = None
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
                if is_null > 2:  # if three cols are null, then del
                    del output_json['line'][idx]
                    break
            count -= 1
        # output file
        return output_json
    
    
    def aa_to_ori(self, aa_json_format):
        """
        Transform AA IQ Bot output json to defined json format
        
        Arguments:
            aa_json_format  -- output json of AA IQ Bot
        
        Returns:
            output_json -- defined json format

        """
        output_json = aa_json_format # 還不知 aa_json_format
        
        return output_json
        

