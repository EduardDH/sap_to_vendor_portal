#!/usr/bin/python

import logging
import time
from hdbcli import dbapi
from hdbcli.dbapi import Error, ProgrammingError
#import config
import json
from datetime import datetime

global conn
global cursor


def hana_db_connect(config):
    try:
        global conn
        global cursor
        conn = dbapi.connect(
            address=config['HANA_DB_cred']['address'],
            port=config['HANA_DB_cred']['port'],
            user=config['HANA_DB_cred']['user'],
            password=config['HANA_DB_cred']['password'],
            connectTimeout=5000,
            reconnect='FALSE'
        )

        # Set HANA DB schema
        cursor = conn.cursor()
        cursor.execute("SET SCHEMA SAPDAT")
        logging.debug('HANA DB connection established')
    except:
        logging.exception('Cannot connect to HANA DB')
        pass


def main_program():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    # Activate error logging
    logging.basicConfig(
        filename=config['LogFile']['path'],
        level=logging.DEBUG,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # HANA DB Connection
    hana_db_connect(config)
    data = {}

    try:
        # VBRP can have multyple records
        cursor.execute("SELECT VBAK.ZBACKEND AS global_entity_id,\
                        VBAK.BSTNK AS order_id,\
                        VBAK.ZTIMESTAMP AS created_at,\
                        VBAK.UPD_TMSTMP AS updated_at,\
                        (0-VBRP.NETWR) AS vendor_net_revenue,\
                        VBAK.ZPAYMENTPROVIDER AS provider,\
                        VBAK.VBELN,\
                        vbak.KNUMV,\
                        VBRK.MWSBK as tax_charge\
                        FROM VBAK\
                        left JOIN VBRP ON VBAK.VBELN  = VBRP.AUBEL\
                        left JOIN VBRK ON VBRP.VBELN  = VBRK.VBELN\
                        WHERE VBAK.BSTNK = 'xyke-a66l'\
                        ORDER by VBRP.VBELN desc"
                       )

        # for num, row in enumerate(cursor):
        row = cursor.fetchone()

        data['global_entity_id'] = row[0]
        data['order_id'] = row[1]
        data['created_at'] = row[2]
        data['updated_at'] = str(row[3])
        data['vendor_net_revenue'] = str(row[4])

        data['payout'] = {}
        data['subtotal'] = {}
        data['payment'] = {}
        data['discount'] = []
        data['delivery'] = {}
        data['voucher'] = {}
        data['commission'] = {}
        data['joker'] = {}
        data['customer_refund'] = {}
        data['vendor_refund'] = {}
        data['customer_fee'] = {}
        data['tip'] = {}
        data['tax'] = {}
        data['vendor_charges'] = {}

        data['payment']['provider'] = row[5]
        data['tax']['tax_charge'] = str(row[8])
        VBAK_VBELN = row[6]
        VBAK_KNUMV = row[7]

        discount_vendor = {}
        discount_plaform = {}
        ZVAM = 0
        ZVA2 = 0
        ZVA3 = 0

        cursor.execute("SELECT KSCHL, KWERT FROM PRCD_ELEMENTS pe \
                WHERE pe.KNUMV  = '" + VBAK_KNUMV + "'"
                       )

        pe = cursor.fetchall()
        for KSCHL, KWERT in pe:
            match KSCHL:
                case 'Z052':
                    data['customer_paid_amount'] = str(0 - KWERT)
                case 'Z04C':
                    data['subtotal']['gross_amount'] = str(KWERT)
                case 'Z022':
                    data['subtotal']['net_amount'] = str(KWERT)
                case 'Z04G':
                    discount_vendor['gross_food_amount'] = str(KWERT)
                case 'Z062':
                    discount_vendor['net_food_amount'] = str(KWERT)
                case 'Z04L':
                    discount_vendor['gross_delivery_amount'] = str(KWERT)
                case 'Z04K':
                    discount_vendor['net_delivery_amount'] = str(KWERT)
                case 'Z04F':
                    discount_plaform['gross_food_amount'] = str(KWERT)
                case 'Z064':
                    discount_plaform['net_food_amount'] = str(KWERT)
                case 'Z079':
                    discount_plaform['gross_delivery_amount'] = str(KWERT)
                case 'Z078':
                    discount_plaform['net_delivery_amount'] = str(KWERT)
                case 'Z04D':
                    data['delivery']['gross_fee'] = str(KWERT)
                case 'ZDF1':
                    data['subtotal']['net_fee'] = str(KWERT)
                case 'Z075':
                    data['voucher']['gross_amount'] = str(KWERT)
                    data['voucher']['owner'] = 'Vendor'
                case 'ZVO1':
                    data['voucher']['net_amount'] = str(KWERT)
                case 'Z077':
                    data['voucher']['gross_amount'] = str(KWERT)
                    data['voucher']['owner'] = 'Platform'
                case 'Z076':
                    data['voucher']['net_amount'] = str(KWERT)
                case 'ZJF1':
                    data['jocker']['gross_fee'] = str(KWERT)
                case 'ZJF2':
                    data['jocker']['net_fee'] = str(KWERT)
                case 'ZSFG':
                    data['customer_fee']['gross_service_fee'] = str(KWERT)
                case 'ZSFN':
                    data['customer_fee']['net_service_fee'] = str(KWERT)
                case 'Z024':
                    data['customer_fee']['gross_container_fee'] = str(KWERT)
                case 'Z04E':
                    data['customer_fee']['gross_mov_fee'] = str(KWERT)
                case 'ZMV0':
                    data['customer_fee']['net_mov_fee'] = str(KWERT)
                case 'ZVAM':
                    ZVAM = KWERT
                case 'ZVA2':
                    ZVA2 = KWERT
                case 'ZVA3':
                    ZVA3 = KWERT

        data['tax']['total_amount'] = str(ZVAM + ZVA2 + ZVA3)

        if 'gross_delivery_amount' in discount_vendor:
            discount_vendor['owner'] = 'Vendor'
            data['discount'].append(discount_vendor)

        if 'gross_delivery_amount' in discount_plaform:
            discount_vendor['owner'] = 'Platform'
            data['discount'].append(discount_plaform)

        json_data = json.dumps(data, sort_keys=False, indent=4)
        print(json_data)
    except:
        logging.exception('HANA DB connection error')


main_program()
