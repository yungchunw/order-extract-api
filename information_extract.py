#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import re
import json
import pandas as pd
import Levenshtein
from dateutil.parser import parse
from hanziconv import HanziConv
#from fuzzywuzzy import fuzz
#sim = round(fuzz.token_sort_ratio(ocr_result, vlue),3)

# gen defined json format


def gen_output_ori(output_json_azure):
    result = output_json_azure['analyzeResult']['documentResults'][0]['fields']
    output_json = {'header': {}, 'line': []}
    # header
    header_list = ['buyerName', 'supplierName', 'custPoNumber', 'poDate',
                   'shipAddr', 'billAddr', 'deliverAddr', 'payCurrency',
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
    line_list = ['lineNumber', 'custPartNo', 'sellingPrice',
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
            if is_null > 1:  # if two cols are null, then del
                del output_json['line'][idx]
                break
        count -= 1
    # output file
    return output_json

# load mapping_list


def load_mapping_list():
    # buyerName
    with open('mapping_list/LIST_buyerName.json', 'r') as f:
        buyerName_list = json.load(f)
    # supplierName
    supplierName_all = []
    with open('mapping_list/supplyer_name.txt', 'r') as f:
        for line in f:
            supplierName_all.append(line.split('\n')[0])
    with open('mapping_list/LIST_supplierName.json', 'r') as f:
        supplierName_list = json.load(f)
    # custPoNumber
    with open('mapping_list/custPoNumber_his.json', 'r') as f:
        custPoNumber_list = json.load(f)
    # address
    with open('mapping_list/LIST_address.json', 'r') as f:
        address_list = json.load(f)
    # payCurrency
    with open('mapping_list/LIST_payCurrency.json', 'r') as f:
        payCurrency_list = json.load(f)
    # paymentTerm
    with open('mapping_list/LIST_paymentTerm.json', 'r') as f:
        paymentTerm_list = json.load(f)
    # tax
    with open('mapping_list/LIST_tax.json', 'r') as f:
        tax_list = json.load(f)
    # tradeTerm
    with open('mapping_list/LIST_tradeTerm.json', 'r') as f:
        tradeTerm_list = json.load(f)
    # lineNumber
    with open('mapping_list/lineNumber_his.json', 'r') as file:
        lineNumber_list = json.load(file)

    # load active cust items
    active_item = pd.read_csv(
        'mapping_list/cust_item_active.txt', delimiter='\t')
    active_item = active_item[active_item.columns].astype(str)
    active_item.drop_duplicates(inplace=True)
    active_item.reset_index(drop=True, inplace=True)
    # print(active_item.info())

    return buyerName_list, supplierName_all, supplierName_list, custPoNumber_list, \
        address_list, payCurrency_list, paymentTerm_list, tax_list, tradeTerm_list, \
        lineNumber_list, active_item

# combainations of string


def get_combinations(string, length=0):
    combs = []
    if length == 0:
        for i in range(len(string)):
            combs.append(string[i:])
    else:
        for i in range((len(string))):
            if len(string[i:i+length]) == length:
                combs.append(string[i:i+length])

    return combs

# --------------------header--------------------
# buyerNme


def gen_buyerName(custID, buyerName_list):
    return buyerName_list[custID][0]

# supplierName


def gen_supplierName(ori_json, ouID, supplierName_all, supplierName_list, threshold=0.5):
    # 名稱清單 supplierName_all
    # 比對清單 supplierName_list
    supplier_name_ocr = ori_json['header']['supplierName']
    sim_1 = 0
    name_find_1 = ''
    if supplier_name_ocr is not None:
        supplier_name_ocr = HanziConv.toTraditional(
            supplier_name_ocr)  # ocr轉換為繁體
        for name in supplierName_all:
            if Levenshtein.ratio(supplier_name_ocr, name) > sim_1:
                sim_1 = Levenshtein.ratio(supplier_name_ocr, name)
                name_find_1 = name
    else:  # 若ocr為null，直接從比對清單中取值
        return supplierName_list[ouID][0]

    sim_2 = 0
    name_find_2 = ''
    for name in supplierName_list[ouID]:
        if Levenshtein.ratio(supplier_name_ocr, name) > sim_2:
            name_find_2 = name

    if (sim_1 > threshold) & (sim_2 > threshold):
        if sim_1 > sim_2:
            return name_find_1
        else:
            return name_find_2
    elif sim_1 > threshold:
        return name_find_1
    elif sim_2 > threshold:
        return name_find_2
    else:  # 相似度皆小於threshold，直接從比對清單中取值
        return supplierName_list[ouID][0]

# custPoNumber


def get_format_of_poNum(custID, custPoNumber_list):
    poNum_his = custPoNumber_list[custID]
    # dict by length
    length_dict = {}
    for poNum in poNum_his:
        if len(poNum) > 3:
            if len(poNum) not in length_dict.keys():
                length_dict[len(poNum)] = [poNum]
            else:
                length_dict[len(poNum)].append(poNum)
    # get format
    po_num_format = {}
    for length in length_dict:
        form = []
        for i in range(length):
            typeOfchar = ''
            for poNum in length_dict[length]:
                char = poNum[i]
                if typeOfchar == '':  # first time
                    if char == '-':
                        typeOfchar = char
                    elif char.isdigit():
                        typeOfchar = '\\d'
                    else:
                        typeOfchar = '\\w'
                else:
                    if (char == '-') & (typeOfchar != '-'):  # 可能為不同格式 or 不同長度
                        typeOfchar = '\\w'
                    elif (char.isdigit() & (typeOfchar == '\\d' or typeOfchar.isdigit())) & (char != typeOfchar):
                        typeOfchar = '\\d'
                    elif char != typeOfchar:
                        typeOfchar = '\\w'
            form.append(typeOfchar)
        po_num_format[length] = form
    return po_num_format


def gen_custPoNumber(ori_json, custID, custPoNumber_list):
    po_num_format = get_format_of_poNum(custID, custPoNumber_list)
    # check ocr by format
    po_num_ocr = ori_json['header']['custPoNumber']
    if po_num_ocr is not None:
        po_num_ocr = ''.join([item for item in list(
            dict.fromkeys(po_num_ocr.split(' ')))])  # drop duplicates
        custPoNumber = ''
        #print('custPoNumber extracting...')
        for length in po_num_format:
            po_num_ocr_combs = get_combinations(po_num_ocr, length)
            # print(po_num_format[length])
            # print(po_num_ocr_combs)
            for string in po_num_ocr_combs:
                is_format = True
                for i in range(len(string)):
                    # 判別格式是否相符
                    #print(po_num_format[length][i], string[i])
                    if string[i] != '-':
                        if (po_num_format[length][i] == '\\d'):
                            if not string[i].isdigit():
                                is_format = False
                                break
                        elif (po_num_format[length][i] == '\\w'):
                            if not string[i].isalnum():
                                is_format = False
                                break
                    else:
                        if (po_num_format[length][i] != '-'):
                            is_format = False
                            break
                if is_format:
                    custPoNumber = string

        # print(custPoNumber)
        return custPoNumber


# Date
def gen_date(dt):

    try:
        dt = dt.split(" ")[0]
        dt = dt.replace(u'年', '-')
        dt = dt.replace(u'月', '-')
        dt = dt.replace(u'日', '-')
        dt = parse(dt)  # 解析日期時間
        if len(str(dt.year)) == 3:
            dt = dt.replace(year=dt.year + 1911)
        date_str = dt.strftime("%Y-%m-%d")

        return date_str
    except:
        return dt

# address


def address_sim(addr_ocr, address_list, custID, code):
    sim = 0
    addr_find = ''
    if addr_ocr is not None:
        addr_ocr = HanziConv.toTraditional(addr_ocr)  # ocr地址轉換為繁體
        for idx in range(len(address_list[custID]['SITE_USE_CODE'])):
            if address_list[custID]['SITE_USE_CODE'][idx] == code:
                addr = address_list[custID]['ADDRESS2'][idx] + \
                    address_list[custID]['ADDRESS3'][idx] + address_list[custID]['ADDRESS4'][idx]
                if Levenshtein.ratio(addr_ocr, addr) > sim:
                    sim = Levenshtein.ratio(addr_ocr, addr)
                    addr_find = addr
    return addr_find, sim


def gen_address(ori_json, custID, ouID, address_list, threshold):
    # shipAddr
    shipAddr_ocr = ori_json['header']['shipAddr']
    shipAddr, shipAddr_sim = address_sim(
        shipAddr_ocr, address_list, custID, 'SHIP_TO')

    # billAddr
    billAddr_ocr = ori_json['header']['billAddr']
    billAddr, billAddr_sim = address_sim(
        billAddr_ocr, address_list, custID, 'BILL_TO')

    if billAddr == '':
        billAddr, billAddr_sim = address_sim(
            shipAddr, address_list, custID, 'BILL_TO')
        if billAddr_sim < threshold:
            billAddr = ''
    # deliverAddr
    deliverAddr_ocr = ori_json['header']['deliverAddr']
    deliverAddr, deliverAddr_sim = address_sim(
        deliverAddr_ocr, address_list, custID, 'DELIVER_TO')

    if deliverAddr == '':
        deliverAddr, deliverAddr_sim = address_sim(
            shipAddr, address_list, custID, 'DELIVER_TO')
        if deliverAddr_sim < threshold:
            deliverAddr = ''

    return shipAddr, billAddr, deliverAddr

# payCurrency


def gen_payCurrency(key, payCurrency_list):

    return payCurrency_list[key]

# paymentTerm & tax & tradeTerm


def gen_term(ori_json, custID, ouID, term_list, term_name):
    if (custID + '_' + ouID) in term_list.keys():
        if ori_json['header'][term_name] is None:
            return term_list[custID + '_' + ouID]
        elif term_list[custID + '_' + ouID] == '':  # 比對清單為null，產出ocr
            return ori_json['header'][term_name]
        else:  # 計算相似度 or 與學習清單比對
            return term_list[custID + '_' + ouID]
    else:  # 若比對清單無此key
        return ori_json['header'][term_name]

# --------------------line--------------------
# lineNumber


def gen_lineNumber(line_ocr, custID, lineNumber_list):
    if custID in lineNumber_list.keys():
        lineNum_his = lineNumber_list[custID][-1]
    else:
        return line_ocr['lineNumber']
    lineNum_format = {len(lineNum_his): []}
    for i in range(len(lineNum_his)):
        typeOfchar = ''
        if lineNum_his[i] not in ['.', '-', '_']:
            if lineNum_his[i].isdigit():
                typeOfchar = '\\d'
            else:
                typeOfchar = '\\w'
        else:
            typeOfchar = lineNum_his[i]
        lineNum_format[len(lineNum_his)].append(typeOfchar)
    # 可能為 1,2,3...,10,...，就需要有兩種format。
    if (len(lineNum_format) == 1) & (list(lineNum_format.keys())[0] == 1):
        lineNum_format[2] = ['\\d', '\\d']
    elif (len(lineNum_format) == 1) & (list(lineNum_format.keys())[0] == 2):
        if (lineNum_format[2][0] in ['.', '-', '_']) or (lineNum_format[2][1] in ['.', '-', '_']):
            pass
        else:
            lineNum_format[2] = ['\\d']
    # gen lineNumber
    lineNumber = ''
    lineNum_ocr = line_ocr['lineNumber']
    if lineNum_ocr is not None:
        lineNum_ocr = ''.join([item for item in list(dict.fromkeys(lineNum_ocr.split(' ')))])  # drop duplicates
        for length in lineNum_format:
            form = True
            if length > len(lineNum_ocr):
                form = False
                break
            for i in range(length):
                if (lineNum_ocr[i] != lineNum_format[length][i]) & (lineNum_format[length][i] != '\\d'):
                    form = False
                    break
            if form:
                lineNumber = lineNum_ocr[:length]
    else:
        lineNum_ocr = line_ocr['custPartNo']  # 可能label到custPartNo中
        if lineNum_ocr is not None:
            lineNum_ocr = ''.join([item for item in list(
                dict.fromkeys(lineNum_ocr.split(' ')))])  # drop duplicates
            for length in lineNum_format:
                form = True
                if length > len(lineNum_ocr):
                    form = False
                    break
                for i in range(length):
                    if (lineNum_ocr[i] != lineNum_format[length][i]) & (lineNum_format[length][i] != '\\d'):
                        form = False
                        break
                if form:
                    lineNumber = lineNum_ocr[:length]
        for synta in ['.', '-', '_']:
            if (synta not in lineNumber) & (not lineNumber.isdigit()):
                lineNumber = ''

    return lineNumber

# custPartNo


def gen_custPartNo(line_ocr, custID, active_item):
    partNo_ocr = line_ocr['custPartNo']
    custPartNo = ''
    print('searching item: %s' % (partNo_ocr))
    if partNo_ocr is not None:
        if len(partNo_ocr) < 4:
            pass
        else:
            sim_custItem = 0
            # if accuracy of ocr result is 100%
            if (partNo_ocr in list(active_item[active_item.CUSTOMER_NUMBER == custID]['CUSTOMER_ITEM_NUMBER'])) or (partNo_ocr in list(active_item[active_item.CUSTOMER_NUMBER == custID]['ITEM_NO'])):
                return partNo_ocr
            for item in active_item[active_item.CUSTOMER_NUMBER == custID]['CUSTOMER_ITEM_NUMBER']:
                sim_tmp = Levenshtein.ratio(partNo_ocr, item)
                if sim_tmp > 0.9:
                    sim_custItem = sim_tmp
                    custPartNo = item
                    return custPartNo
                elif sim_tmp > sim_custItem:
                    sim_custItem = sim_tmp
                    custPartNo = item
            sim_item = 0
            for item in active_item[active_item.CUSTOMER_NUMBER == custID]['ITEM_NO']:
                sim_tmp = Levenshtein.ratio(partNo_ocr, item)
                if sim_tmp > 0.9:
                    sim_item = sim_tmp
                    custPartNo = item
                    return custPartNo
                elif sim_tmp > sim_item:
                    sim_item = sim_tmp
                    custPartNo = item
    else:
        partNo_ocr = line_ocr['lineNumber']  # 可能label到lineNumber中
        print('searching item:%s' % (partNo_ocr))
        if partNo_ocr is not None:
            if len(partNo_ocr) < 4:
                pass
            else:
                sim_custItem = 0
                for item in active_item[active_item.CUSTOMER_NUMBER == custID]['CUSTOMER_ITEM_NUMBER']:
                    sim_tmp = Levenshtein.ratio(partNo_ocr, item)
                    if sim_tmp > 0.9:
                        sim_custItem = sim_tmp
                        custPartNo = item
                        return custPartNo
                    if sim_tmp > sim_custItem:
                        sim_custItem = sim_tmp
                        custPartNo = item
                sim_item = 0
            for item in active_item[active_item.CUSTOMER_NUMBER == custID]['ITEM_NO']:
                sim_tmp = Levenshtein.ratio(partNo_ocr, item)
                if sim_tmp > 0.9:
                    sim_item = sim_tmp
                    custPartNo = item
                    return custPartNo
                elif sim_tmp > sim_item:
                    sim_item = sim_tmp
                    custPartNo = item
    return custPartNo

# sellingPrice


def gen_sellingPrice(line_ocr):
    price_ocr = line_ocr['sellingPrice']
    if price_ocr is not None:
        for syntax in ['$', ',']:  # remove syntax
            price_ocr.replace(syntax, '')
        price_ocr = ''.join([item for item in list(
            dict.fromkeys(price_ocr.split(' ')))])  # drop duplicates
        sellingPrice = []
        for price_ocr_idx in price_ocr.split('.'):
            price_ocr_combs = get_combinations(price_ocr_idx)
            max_length = 0
            price = ''
            for string in price_ocr_combs:
                if string.isdigit():
                    if len(string) > max_length:
                        max_length = len(string)
                        price = string
            sellingPrice.append(price)
        return float('.'.join(sellingPrice))

# voQty


def gen_voQty(line_ocr):
    qty_ocr = line_ocr['voQty']
    #print('voQty extract...')
    if qty_ocr is not None:
        for syntax in [',']:  # remove syntax
            qty_ocr = qty_ocr.replace(syntax, '')
        qty_ocr = ''.join([item for item in list(
            dict.fromkeys(qty_ocr.split(' ')))])  # drop duplicates
        # print(qty_ocr)
        qty_ocr = qty_ocr.split('.')[0]
        qty_ocr = [re.search('[0-9]*', qty_ocr).group()]
        # print(qty_ocr)
        voQty = 0
        max_length = 0
        for qty_ocr_idx in qty_ocr:
            qty_ocr_combs = get_combinations(qty_ocr_idx)
            for string in qty_ocr_combs:
                if string.isdigit():
                    if len(string) > max_length:
                        max_length = len(string)
                        voQty = int(string)
        return voQty
    else:
        return 0


def extract_info(azure_json, file_name):
    ori_json = gen_output_ori(azure_json)
    # mapping list loading
    print('mapping list loading...')
    buyerName_list, supplierName_all, supplierName_list, custPoNumber_list, \
        address_list, payCurrency_list, paymentTerm_list, tax_list, tradeTerm_list, \
        lineNumber_list, active_item = load_mapping_list()

    custID = file_name.split('_')[2]
    ouID = file_name.split('_')[3]
    key = custID + '_' + ouID

    output_json = {}
    print('header extracting...')
    # header
    output_json['header'] = {}
    output_json['header']['buyerName'] = gen_buyerName(custID, buyerName_list)
    output_json['header']['supplierName'] = gen_supplierName(
        ori_json, ouID, supplierName_all, supplierName_list)
    output_json['header']['custPoNumber'] = gen_custPoNumber(
        ori_json, custID, custPoNumber_list)
    # undo
    output_json['header']['poDate'] = gen_date(ori_json['header']['poDate'])
    output_json['header']['shipAddr'], output_json['header']['billAddr'], \
        output_json['header']['deliverAddr'] = gen_address(
            ori_json, custID, ouID, address_list, 0.6)
    output_json['header']['payCurrency'] = gen_payCurrency(
        key, payCurrency_list)
    output_json['header']['paymentTerm'] = gen_term(
        ori_json, custID, ouID, paymentTerm_list, 'paymentTerm')
    output_json['header']['tax'] = gen_term(
        ori_json, custID, ouID, tax_list, 'tax')
    output_json['header']['tradeTerm'] = gen_term(
        ori_json, custID, ouID, tradeTerm_list, 'tradeTerm')

    print('line extracting...')
    # line
    output_json['line'] = []
    count = 1
    for row in ori_json['line']:
        print('line', count)
        line_info = {}
        line_info['lineNumber'] = gen_lineNumber(row, custID, lineNumber_list)
        # in processing
        line_info['custPartNo'] = gen_custPartNo(row, custID, active_item)
        print('custPartN is %s' % line_info['custPartNo'])
        line_info['sellingPrice'] = gen_sellingPrice(row)
        line_info['originalRequestDate'] = gen_date(row['originalRequestDate'])
        # undo
        line_info['voQty'] = gen_voQty(row)
        output_json['line'].append(line_info)
        count += 1
    return output_json


if __name__ == '__main__':
    #file_name = '5210_63144_9596_317_ori.json'
    start = time.time()
    input_path = 'output_ori/'
    output_path = 'results/'
    file_name = '.json'

    file_start_ = time.time()
    print(file_name)
    with open(input_path + file_name, 'r') as f:
        ori_json = json.load(f)
    output_json = extract_info(ori_json, file_name.split('.json')[0])

    with open(output_path + file_name.replace('_ori', ''), 'w') as output_file:
        json.dump(output_json, output_file, ensure_ascii=False, indent=4)

    end = time.time()
    print('total time:', (end - start))
