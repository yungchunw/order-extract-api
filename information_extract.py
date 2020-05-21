 #!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import re
import json
import argparse
import pandas as pd
import Levenshtein
from dateutil.parser import parse
from hanziconv import HanziConv
#from fuzzywuzzy import fuzz
#sim = round(fuzz.token_sort_ratio(ocr_result, vlue),3)
from format_transform import gen_defined_output
from util_lib import log_util

# --------------------functions--------------------


def load_mapping_list():
    """
    loading all mapping list

    Arguments:


    Returns:
        all mapping list

    """
    mapping_list_all = {}
    
    # buyerName
    with open('mapping_list/LIST_buyerName.json', 'r') as f:
        buyerName_list = json.load(f)
        mapping_list_all['buyerName_list'] = buyerName_list
    # supplierName
    supplierName_all = []
    with open('mapping_list/supplyer_name.txt', 'r') as f:
        for line in f:
            supplierName_all.append(line.split('\n')[0])
            mapping_list_all['supplierName_all'] = supplierName_all
    with open('mapping_list/LIST_supplierName.json', 'r') as f:
        supplierName_list = json.load(f)
        mapping_list_all['supplierName_list'] = supplierName_list
    # custPoNumber
    with open('mapping_list/custPoNumber_his.json', 'r') as f:
        custPoNumber_list = json.load(f)
        mapping_list_all['custPoNumber_list'] = custPoNumber_list
    # address
    with open('mapping_list/LIST_address.json', 'r') as f:
        address_list = json.load(f)
        mapping_list_all['address_list'] = address_list
    # payCurrency
    with open('mapping_list/LIST_payCurrency.json', 'r') as f:
        payCurrency_list = json.load(f)
        mapping_list_all['payCurrency_list'] = payCurrency_list
    # paymentTerm
    with open('mapping_list/LIST_paymentTerm.json', 'r') as f:
        paymentTerm_list = json.load(f)
        mapping_list_all['paymentTerm_list'] = paymentTerm_list
    # tax
    with open('mapping_list/LIST_tax.json', 'r') as f:
        tax_list = json.load(f)
        mapping_list_all['tax_list'] = tax_list
    # tradeTerm
    with open('mapping_list/LIST_tradeTerm.json', 'r') as f:
        tradeTerm_list = json.load(f)
        mapping_list_all['tradeTerm_list'] = tradeTerm_list
    # lineNumber
    with open('mapping_list/lineNumber_his.json', 'r') as file:
        lineNumber_list = json.load(file)
        mapping_list_all['lineNumber_list'] = lineNumber_list

    # load active cust items
    active_item = pd.read_csv(
        'mapping_list/cust_item_active.txt', delimiter='\t')
    active_item = active_item[active_item.columns].astype(str)
    active_item.drop_duplicates(inplace=True)
    active_item.reset_index(drop=True, inplace=True)
    # print(active_item.info())
    
    mapping_list_all['active_item'] = active_item

    
    return mapping_list_all


def get_combinations(string, length=0):
    """
    generating combinations of a string

    Arguments:
        string -- str
        length -- length of 'return string', if 0 return all combinations

    Returns:
        combinations of string

    """
    
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


def gen_buyerName(custID, buyerName_list):
    """
    buyerName

    Arguments:
        custID -- cust id
        buyerName_list -- a mapping list of buyerName

    Returns:
        a value of buyerName

    """
    
    return buyerName_list[custID][0]


def gen_supplierName(ori_json, ouID, supplierName_all, supplierName_list, threshold=0.5):
    """
    supplierName

    Arguments:
        ori_json -- a defined json
        ouID -- supplier id
        supplierName_all -- a list of all supplier's name in WPG
        supplierName_list -- a mapping list of supplierName
        thershold - folat

    Returns:
        a value of supplierName

    """
    # 名稱清單 supplierName_all
    # 比對清單 supplierName_list
    supplier_name_ocr = ori_json['header']['supplierName']
    print(supplier_name_ocr)
    sim_1 = 0
    name_find_1 = ''
    if supplier_name_ocr is not None:
        supplier_name_ocr = HanziConv.toTraditional(
            supplier_name_ocr)  # ocr轉換為繁體
        for name in supplierName_all:
            if Levenshtein.ratio(supplier_name_ocr.lower(), name.lower()) > sim_1:
                sim_1 = Levenshtein.ratio(supplier_name_ocr.lower(), name.lower())
                name_find_1 = name
    else:  # 若ocr為null，直接從比對清單中取值
        return supplierName_list[ouID][0]

    sim_2 = 0
    name_find_2 = ''
    for name in supplierName_list[ouID]:
        if Levenshtein.ratio(supplier_name_ocr.lower(), name.lower()) > sim_2:
            sim_2 = Levenshtein.ratio(supplier_name_ocr.lower(), name.lower())
            name_find_2 = name

    print(name_find_1, sim_1)
    print(name_find_2, sim_2)
    print(supplierName_list[ouID][0])
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


def get_format_of_poNum(custID, custPoNumber_list):
    """
    generating formats of custPoNumber

    Arguments:
        custID -- cust id
        custPoNumber_list -- a list of custPoNumber history in ERP

    Returns:
        formats of custPoNumber

    """
    
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
                        typeOfchar = '\\D'
                else:
                    if (char == '-') & (typeOfchar != '-'):  # 可能為不同格式 or 不同長度
                        typeOfchar = '\\D'
                    elif (char.isdigit() & (typeOfchar == '\\d' or typeOfchar.isdigit())) & (char != typeOfchar):
                        typeOfchar = '\\d'
                    elif char != typeOfchar:
                        typeOfchar = '\\D'
            form.append(typeOfchar)
        po_num_format[length] = form
    return po_num_format


def gen_custPoNumber(ori_json, custID, custPoNumber_list):
    """
    custPoNumber

    Arguments:
        ori_json -- a defined json
        custID -- cust id
        custPoNumber_list -- a list of custPoNumber history in ERP

    Returns:
        a value of custPoNumber

    """
    
    po_num_format = get_format_of_poNum(custID, custPoNumber_list)
    # check ocr by format
    po_num_ocr = ori_json['header']['custPoNumber']
    custPoNumber = ''
    if po_num_ocr is not None:
        po_num_ocr = ''.join([item for item in list(
            dict.fromkeys(po_num_ocr.split(' ')))])  # drop duplicates
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
                        elif (po_num_format[length][i] == '\\D'):
                            if not string[i].isalnum():
                                is_format = False
                                break
                    else:
                        if (po_num_format[length][i] != '-'):
                            is_format = False
                            break
                if is_format & (len(string) > len(custPoNumber)):
                    #print(po_num_format[length], string)
                    custPoNumber = string
                    break
        
        print(po_num_ocr, custPoNumber)
    return custPoNumber


def gen_date(dt_ocr):
    """
    poDate, originalRequestDate

    Arguments:
        dt_ocr -- ocr result of date

    Returns:
        a value of poDate or originalRequestDate

    """
    verify_date = ['2016', '2017', '2018', '2019', '2020', '2021', '2022']
    if dt_ocr is not None:
        for synta in ['/', ' ', '年', '月', '日']:
            dt_ocr = dt_ocr.replace(synta, '-')
        print(dt_ocr)
        dt_ocr_combs = get_combinations(dt_ocr)
        date = ''
        for dt in dt_ocr_combs:
            if len(dt) > 7:
                try:
                    dt = parse(dt)  # 解析日期時間
                    if len(str(dt.year)) == 3:
                        dt = dt.replace(year=dt.year + 1911)
                    date = dt.strftime("%Y-%m-%d")
                    break
                except:
                    pass
        # 驗證日期合理性
        date_tmp = date.replace('-', '')
    
        if (date.split('-')[0] in verify_date) & (len(date_tmp) > 7):
            pass 
        
            if (('20' + date.split('-')[2]) in verify_date) & (date.split('-')[0] != ('20' + date.split('-')[2])):
                # 如果"日/月/年"跟“年/月/日”兩者不相同則兩個日期都產出
                #date = [date, '20' + date.split('-')[2] + '-' + date.split('-')[1] + '-' + date.split('-')[0][-2:]]
                # 警告日月年與年月日可能相反
                #date += '(Warning)'
                pass
        else:
            if date_tmp.isdigit() & (len(date_tmp) > 7):
                date = '20' + date.split('-')[2] + '-' + date.split('-')[1] + \
                    '-' + date.split('-')[0][-2:]
    else:
        date = ''
    print(date)
    return date


def address_sim(addr_ocr, address_list, custID, code, threshold=0.2):
    """
    calculate similarity of address

    Arguments:
        addr_ocr -- a ocr result of address
        address_list -- a mapping list of address
        custID -- cust id
        code -- SHIP_TO or BILL_TO or DELIVER_TO

    Returns:
        the address in address_list which has highest similarity, similarity 

    """
    sim = 0
    addr_find = ''
    if (addr_ocr is not None) & (addr_ocr != ''):
        addr_ocr = HanziConv.toTraditional(addr_ocr)  # ocr地址轉換為繁體
        for idx in range(len(address_list[custID]['SITE_USE_CODE'])):
            if address_list[custID]['SITE_USE_CODE'][idx] == code:
                # ADDRESS 2, 3, 4
                addr1 = address_list[custID]['ADDRESS2'][idx] + \
                    address_list[custID]['ADDRESS3'][idx] + address_list[custID]['ADDRESS4'][idx]
                # ADDRESS 2, 3
                addr2 = address_list[custID]['ADDRESS2'][idx] + \
                    address_list[custID]['ADDRESS3'][idx]
                # ADDRESS 2
                addr3 = address_list[custID]['ADDRESS2'][idx]
  
                sim1 = Levenshtein.ratio(addr_ocr.lower(), addr1.lower())
                sim2 = Levenshtein.ratio(addr_ocr.lower(), addr2.lower())
                sim3 = Levenshtein.ratio(addr_ocr.lower(), addr3.lower())

                addrs = [addr1, addr2, addr3]
                for idx, sim_i in enumerate([sim1, sim2, sim3]):
                    if sim_i > sim:
                        sim = sim_i
                        addr_find = addrs[idx]
                
            
      
        
    #print(addr_find, sim)
    return addr_find, sim


def gen_address(ori_json, custID, ouID, address_list, threshold):
    """
    shipAddr, billAddr, deliverAddr

    Arguments:
        ori_json -- a defined json
        custID -- cust id
        ouID -- supplier id
        address_list -- a mapping list of address
        threshold -- float

    Returns:
        values of shipAddr, billAddr, deliverAddr

    """
    
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
    print('ship addr similarity: {}'.format(shipAddr_sim))
    print(shipAddr_ocr)
    print(shipAddr)
    print('bill addr similarity: {}'.format(billAddr_sim))
    print(billAddr_ocr)
    print(billAddr)
    return shipAddr, billAddr, deliverAddr


def gen_payCurrency(key, payCurrency_list):
    """
    payCurrency

    Arguments:
        key -- combination of cust_id + ou_id
        payCurrency_list -- a mapping list of payCurrency

    Returns:
        a value of payCurrency

    """

    return payCurrency_list[key]


def gen_term(ori_json, custID, ouID, term_list, term_name):
    """
    paymentTerm, tax, tradetTerm

    Arguments:
        line_ocr -- a dictionary of line
        custID -- cust id
        term_list -- a mapping list of term
        term_name -- paymentTerm or tax or tradeTerm

    Returns:
        a value of paymentTerm or tax or tradeTerm

    """
    
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


def gen_lineNumber(line_ocr, custID, lineNumber_list):
    """
    lineNumber

    Arguments:
        line_ocr -- a dictionary of line
        custID -- cust id
        lineNumber_list -- a list of lineNumber history in ERP

    Returns:
        a value of lineNumber

    """
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
    # 可能為 1,2,3...,10,...,101， 需要有3種format
    if (len(lineNum_format) == 1) & (list(lineNum_format.keys())[0] == 1):
        lineNum_format[2] = ['\\d', '\\d']
        lineNum_format[3] = ['\\d', '\\d', '\\d']
    elif (len(lineNum_format) == 1) & (list(lineNum_format.keys())[0] == 2):
        if (lineNum_format[2][0] in ['.', '-', '_']) or (lineNum_format[2][1] in ['.', '-', '_']):
            pass
        else:
            lineNum_format[1] = ['\\d']
            lineNum_format[3] = ['\\d', '\\d', '\\d']
            
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
                if (lineNum_format[length][i] == '\\d') & (not lineNum_ocr[i].isdigit()):
                    print(lineNum_format[length][i], lineNum_ocr[i], 'False')
                    form = False
                    break
            # 取格式較長的為主
            if form & (len(lineNumber) < length):
                lineNumber = lineNum_ocr[:length]
    
    else:
        lineNum_ocr = line_ocr['custPartNo']  # 可能label到custPartNo中
        if lineNum_ocr is not None:
            lineNum_ocr = lineNum_ocr.split(' ')[0] # 通常在custPartNo前面
            for length in lineNum_format:
                form = True
                if length > len(lineNum_ocr):
                    form = False
                    break
                for i in range(length):
                    if (lineNum_format[length][i] == '\\d') & (not lineNum_ocr[i].isdigit()):
                        form = False
                        break
                # 取格式較長的為主
                if form & (len(lineNumber) < length):
                    lineNumber = lineNum_ocr[:length]
                    
        for synta in ['.', '-', '_']:
            if (synta not in lineNumber) & (not lineNumber.isdigit()):
                lineNumber = ''
    print('format: {}'.format(lineNum_format))
    print(lineNum_ocr, lineNumber)
    return lineNumber


def gen_custPartNo(line_ocr, custID, active_item):
    """
    custPartNo

    Arguments:
        line_ocr -- a dictionary of line
        custID -- cust id
        active_item -- a mapping list of actived items

    Returns:
        a value of custPartNo

    """
    partNo_ocr = line_ocr['custPartNo']
    custPartNo = ''
    sim_item = 0.0
    print('searching item: %s' % (partNo_ocr))
    if partNo_ocr is not None:
        if len(partNo_ocr) < 4:
            pass
        else:
            # if accuracy of ocr result is 100%
            if (partNo_ocr in list(active_item[active_item.CUSTOMER_NUMBER == custID]['CUSTOMER_ITEM_NUMBER'])) or (partNo_ocr in list(active_item[active_item.CUSTOMER_NUMBER == custID]['ITEM_NO'])):
                return partNo_ocr
            for item in active_item[active_item.CUSTOMER_NUMBER == custID]['CUSTOMER_ITEM_NUMBER']:
                sim_tmp = Levenshtein.ratio(partNo_ocr, item)
                if sim_tmp > 0.9:
                    sim_item = sim_tmp
                    custPartNo = item
                    break
                elif sim_tmp > sim_item:
                    sim_item = sim_tmp
                    custPartNo = item
            # 客料相似度太低時，就找相對應的內料
            if sim_item < 0.5:
                for item in active_item[active_item.CUSTOMER_NUMBER == custID]['ITEM_NO']:
                    sim_tmp = Levenshtein.ratio(partNo_ocr, item)
                    if sim_tmp > 0.9:
                        sim_item = sim_tmp
                        custPartNo = item
                        break
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
                for item in active_item[active_item.CUSTOMER_NUMBER == custID]['CUSTOMER_ITEM_NUMBER']:
                    sim_tmp = Levenshtein.ratio(partNo_ocr, item)
                    if sim_tmp > 0.9:
                        sim_item = sim_tmp
                        custPartNo = item
                        return custPartNo
                    if sim_tmp > sim_item:
                        sim_item = sim_tmp
                        custPartNo = item
                # 客料相似度太低時，就找相對應的內料
                if sim_item < 0.5:
                    for item in active_item[active_item.CUSTOMER_NUMBER == custID]['ITEM_NO']:
                        sim_tmp = Levenshtein.ratio(partNo_ocr, item)
                        if sim_tmp > 0.9:
                            sim_item = sim_tmp
                            custPartNo = item
                            return custPartNo
                        elif sim_tmp > sim_item:
                            sim_item = sim_tmp
                            custPartNo = item
    print(custPartNo, sim_item)
    if (custPartNo == '') & (partNo_ocr is not None):
        if len(partNo_ocr) > 4:
            custPartNo = partNo_ocr
    # model標到奇怪的值的情況
    elif ((custPartNo != '') & (sim_item < 0.5)):
        custPartNo = ''
    return custPartNo


def gen_sellingPrice(line_ocr):
    """
    sellingPrice

    Arguments:
        line_ocr -- a dictionary of line

    Returns:
        a value of sellingPrice

    """
    price_ocr = line_ocr['sellingPrice']
    price = 0.0
    if price_ocr is not None:
        for syntax in ['$', ',']:  # remove syntax
            price_ocr = price_ocr.replace(syntax, '')
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
            if price == '':
                price = '0'
            sellingPrice.append(price)
            print(sellingPrice)
        # 如果目前邏輯無法驗證，則出0
        if len(sellingPrice) > 2:
            price = 0.0
        else:
            price = float('.'.join(sellingPrice))
    print(price_ocr, price)
    return price
        

def gen_voQty(line_ocr):
    """
    voQty

    Arguments:
        line_ocr -- a dictionary of line

    Returns:
        a value of voQty

    """
    qty_ocr = line_ocr['voQty']
    #print('voQty extract...')
    if qty_ocr is not None:
        for syntax in [',']:  # remove syntax
            qty_ocr = qty_ocr.replace(syntax, '')
        qty_ocr = ''.join([item for item in list(
            dict.fromkeys(qty_ocr.split(' ')))])  # drop duplicates
        # print(qty_ocr)
        qty_ocr = qty_ocr.split('.')[0]
        qty_ocr = [re.search('[0-9]*', qty_ocr).group()] # only consider number 0-9 of qty_ocr
        # print(qty_ocr)
        voQty = 0
        max_length = 0
        for qty_ocr_idx in qty_ocr:
            qty_ocr_combs = get_combinations(qty_ocr_idx) # generate different combinations of string
            for string in qty_ocr_combs:
                if string.isdigit():
                    if len(string) > max_length:
                        max_length = len(string)
                        voQty = int(string)
        return voQty
    else:
        return 0

@log_util.debug
def extract_info(raw_json, file_name, mapping_list_all, extras):
    """
    main function of information_extract.py.
    

    Arguments:
        raw_json -- an output json from different services
        file_name -- the input file name

    Returns:
        output_json - a json file after information extracted

    """
    ori_json = gen_defined_output.azure_to_ori(raw_json)
    # mapping list loading
    buyerName_list = mapping_list_all['buyerName_list']
    supplierName_all = mapping_list_all['supplierName_all']
    supplierName_list = mapping_list_all['supplierName_list']
    custPoNumber_list = mapping_list_all['custPoNumber_list']
    address_list = mapping_list_all['address_list']
    payCurrency_list = mapping_list_all['payCurrency_list']
    paymentTerm_list = mapping_list_all['paymentTerm_list']
    tax_list = mapping_list_all['tax_list']
    tradeTerm_list = mapping_list_all['tradeTerm_list']
    lineNumber_list = mapping_list_all['lineNumber_list']
    active_item = mapping_list_all['active_item']
    
    # custID 與 ouID 位置可能會變
    custID = file_name.split('_')[0]
    ouID = file_name.split('_')[1]
    key = custID + '_' + ouID

    output_json = {}
    log_util.logger.debug('header extracting...', extra=extras)
    # header
    output_json['header'] = {}
    output_json['header']['buyerName'] = gen_buyerName(custID, buyerName_list)
    output_json['header']['supplierName'] = gen_supplierName(
        ori_json, ouID, supplierName_all, supplierName_list)
    output_json['header']['custPoNumber'] = gen_custPoNumber(
        ori_json, custID, custPoNumber_list)
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

    log_util.logger.debug('line extracting...', extra=extras)
    # line
    output_json['line'] = []
    request_date = ''
    for row in ori_json['line']:
        print('line')
        line_info = {}
        line_info['lineNumber'] = gen_lineNumber(row, custID, lineNumber_list)
        line_info['custPartNo'] = gen_custPartNo(row, custID, active_item)
        print('custPartN is %s' % line_info['custPartNo'])
        line_info['sellingPrice'] = gen_sellingPrice(row)
        line_info['originalRequestDate'] = gen_date(row['originalRequestDate'])
        # 如果oringinalRequestDate為空值，則套用第一個不為null之日期。
        if (request_date == '') & ((line_info['originalRequestDate'] != '') or (line_info['originalRequestDate'] is not None)):
            request_date = line_info['originalRequestDate']
        line_info['voQty'] = gen_voQty(row)
        output_json['line'].append(line_info)

    # check line
    count = len(output_json['line'])
    while count > 0:
        for k in range(len(output_json['line'])):
            # 若有3個欄位為空值則刪除此line
            is_null = 0
            for value in output_json['line'][k].values():
                if (value == '') or (value == 0.0):
                    is_null += 1
            if is_null > 2: # if fo
                del output_json['line'][k]
                break
            # update oringinalRequestDate (無腦補日期)
            if ((output_json['line'][k]['originalRequestDate'] == '') or (output_json['line'][k]['originalRequestDate'] is None)):
                output_json['line'][k]['originalRequestDate'] = request_date
        count -= 1
    return output_json


if __name__ == '__main__':
    """
    
    Running in local.
    
    Arguments:
        path  -- path of input json file
        
    output:
        output_json -- a json file after information extracted

    """
    start = time.time()
    

    parser = argparse.ArgumentParser(description='information extract')
    parser.add_argument(
         '-p', '--path', required=True, type=str, help='input path')
    args = parser.parse_args()
    
    input_file = args.path

    print('{}\n'.format(input_file.split('/')[-1]))
    print('mapping list loading...')
    mapping_list_all = load_mapping_list()
    with open(input_file, 'r') as f:
        ori_json = json.load(f)
    output_json = extract_info(ori_json, input_file.split('/')[-1].split('.json')[0], mapping_list_all)

    with open('./' + input_file.split('/')[-1], 'w') as output_file:
        json.dump(output_json, output_file, ensure_ascii=False, indent=4)

    print('total time:', (time.time() - start))
