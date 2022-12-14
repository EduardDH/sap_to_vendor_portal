#!/usr/bin/python

from hdbcli import dbapi
from hdbcli.dbapi import Error, ProgrammingError
import json
import logging
import csv


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
    # Read input.csv
    with open('order_list.csv', newline='') as f:
        reader = csv.reader(f)
        order_list = list(reader)

    # HANA DB Connection
    hana_db_connect(config)

    try:
        for order_id in order_list:
            # VBRP can have multyple records
            cursor.execute("SELECT VBAK.ZBACKEND AS global_entity_id,\
                            VBAK.BSTNK AS order_id,\
                            VBAK.ZTIMESTAMP AS created_at,\
                            VBAK.UPD_TMSTMP AS updated_at,\
                            VBAK.NETWR AS vendor_net_revenue,\
                            VBAK.ZPAYMENTPROVIDER AS provider,\
                            VBAK.VBELN,\
                            VBAK.KNUMV,\
                            VBAP.MWSBP as tax_charge,\
                            VBAK.VBELN as dummy,\
                            VBKD.BSARK as is_cash, \
                            VBAK.AUART,\
                            VBAP.ARKTX as reason,\
                            VBAK.ABSTK as is_cancelled,\
                            VBAP.ABGRU as canc_status,\
                            VBAP.PSTYV\
                            FROM VBAK\
                            left JOIN VBRP ON VBAK.VBELN  = VBRP.AUBEL\
                            left JOIN VBKD ON VBKD.VBELN  = VBAK.VBELN\
                            left JOIN VBAP ON VBAP.VBELN  = VBAK.VBELN\
                            WHERE VBAK.BSTNK LIKE '" + order_id[0] + "%'\
                            ORDER by VBRP.VBELN desc"
                           )
            data = {}

            rows = cursor.fetchall()

            if len(rows) == 0:
                print(order_id[0] + ' not found!')
                continue

            if len(rows) == 1:
                row = rows[0]
                if row[13] == '99':
                    is_cancelled = 'TRUE'
                else:
                    is_cancelled = 'FALSE'
            else:  # Count number of ZP2 for order_id*, if Even, then order is cancelled
                for row in rows:
                    zp2_counter = 0
                    if row[14] == 'ZP2':
                        zp2_counter += 1
                if zp2_counter % 2 == 0:
                    is_cancelled = 'TRUE'
                else:
                    is_cancelled = 'FALSE'

                # TODO: fix array sorting
                # sort by VBAK.VBELN
                #rows = rows[rows[:, 6].argsort()]
                # sort by VBAK.BSTNK
                #rows = rows[rows[:, 1].argsort(kind='mergesort')]
                row = rows[len(rows)-1]

            # Order level
            data['global_entity_id'] = row[0]
            data['vendor_id'] = row[1][0: 4]
            data['order_id'] = row[1]
            data['place_timestamp'] = ''
            data['created_at'] = row[2]
            data['timestamp'] = str(row[3])

            global_entity_id = row[0]

            if data['global_entity_id'] == 'FOODPANDA_TW':
                dec_places = 100
            else:
                dec_places = 1

            data['currency'] = 'TODO'
            data['vendor_payout'] = str(-1*dec_places*row[4])

            data['customer_paid_amount'] = 'TODO'
            data['vendor_net_revenue'] = 'TODO'
            data['is_cancelled'] = is_cancelled

            data['is_vendor_payout_paid'] = 'TODO'
            data['payout_transaction_id'] = 'TODO'

            #data['vendor_net_revenue'] = str(-1*dec_places*row[4])
            # if row[12] == 'C':
            #     data['is_cancelled'] = 'TRUE'
            # else:
            #     data['is_cancelled'] = 'FALSE'

            # Initialize after filling order level to keep right order of fields
            data['order_values'] = {}
            data['revenue'] = {}
            data['payment'] = {}

            # data['discount'] = []

            discount_vendor = {}
            discount_plaform = {}
            commission_standard = {}
            commission_fixed = {}
            commission_tiers = {}

            # data['payout']['vendor_amount'] = data['vendor_net_revenue']
            # data['payment']['provider'] = row[5]

            # if row[10] == '01':
            #     data['payment']['is_cash'] = 'FALSE'
            # elif row[10] == '02':
            #     data['payment']['is_cash'] = 'TRUE'

            data['tax']['tax_charge'] = str(row[8])
            VBRP_NETWR = row[4]
            VBAK_VBELN = row[6]
            VBAK_KNUMV = row[7]
            VBAK_AUART = row[11]
            VBAP_ARKTX = row[12]

            ZVAM = 0
            ZVA2 = 0
            ZVA3 = 0
            ZTP1_2 = 0

            # DFKKOP_XBLNR =
            # total voucher+discount (when owner = vendor )
            data['order_values']['incentive_value_gross_vendor'] = 'TODO'
            data['order_values']['incentive_value_net_vendor'] = 'TODO'
            data['order_values']['incentive_value_gross_platform'] = 'TODO'
            data['order_values']['incentive_value_net_platform'] = 'TODO'
            data['order_values']['incentive_value_gross_partner'] = 'TODO'
            data['order_values']['incentive_value_net_partner'] = 'TODO'
            data['order_values']['incentives'] = []

            cursor.execute("SELECT KSCHL, KWERT, KBETR FROM PRCD_ELEMENTS pe \
                    WHERE pe.KNUMV  = '" + VBAK_KNUMV + "'"
                           )

            pe = cursor.fetchall()
            for KSCHL, KWERT, KBETR in pe:
                KWERT = dec_places*KWERT
                match KSCHL:
                    # Order_values Subtotal
                    case 'Z04C':
                        data['order_values']['basket_value_gross'] = str(KWERT)
                    case 'Z022':
                        data['order_values']['basket_value_net'] = str(KWERT)
                    # Discount
                    case 'Z04G':
                        incentives_food_vendor_gross_amount = str(KWERT)
                    case 'Z062':
                        incentives_food_vendor_net_amount = str(KWERT)
                    case 'Z04L':
                        incentives_delivery_vendor_gross_amount = str(KWERT)
                    case 'Z04K':
                        incentives_delivery_vendor_net_amount = str(KWERT)
                    case 'Z04F' | 'ZC01':  # ZC01 for TW, incl all discounts and vouchers
                        incentives_food_platform_gross_amount = str(KWERT)
                    case 'Z064':
                        incentives_food_platform_net_amount = str(KWERT)
                    case 'Z079':
                        incentives_delivery_platform_gross_amount = str(KWERT)
                    case 'Z078':
                        incentives_delivery_platform_net_amount = str(KWERT)
                    # Voucher
                    case 'Z075':
                        incentives_voucher_vendor_gross_amount = str(KWERT)
                    case 'ZVO1':
                        incentives_voucher_vendor_net_amount = str(KWERT)
                    case 'Z077':
                        incentives_voucher_platform_gross_amount = str(KWERT)
                    case 'Z076':
                        incentives_voucher_platform_net_amount = str(KWERT)
                    # Delivery
                    case 'Z04D':
                        data['order_values']['delivery_fee_gross'] = str(KWERT)
                    case 'ZDF1':
                        data['order_values']['delivery_fee_net'] = str(KWERT)
                    # Customer Fee
                    case 'Z024':
                        data['order_values']['container_charges_gross'] = str(
                            KWERT)
                    case 'Z07C':
                        data['order_values']['container_charges_net'] = str(
                            KWERT)
                    case 'Z04E':
                        data['order_values']['mov_surcharge_fee_gross'] = str(
                            KWERT)
                    case 'ZMV0':
                        data['order_values']['mov_surcharge_fee_net'] = str(
                            KWERT)

                    # Tip
                    case 'Z074':
                        data['order_values']['tip_value_gross'] = str(KWERT)
                    case 'ZTP1':
                        data['order_values']['tip_value_net'] = str(KWERT)

                    # Customer paid amount, payment type
                    case 'Z052':
                        data['customer_paid_amount'] = str(0 - KWERT)
                        # data['payment']['type'] = 'online'
                        data['revenue']['payment_type'] = 'online'
                    case 'Z051':
                        data['customer_paid_amount'] = str(0 - KWERT)
                        # data['payment']['type'] = 'corporate'
                        data['revenue']['payment_type'] = 'corporate'
                    case 'Z050' | 'Z053':
                        data['customer_paid_amount'] = str(0 - KWERT)
                        # data['payment']['type'] = 'cash'
                        data['revenue']['payment_type'] = 'cash'
                    case 'ZOC2':
                        ZOC2 = KWERT
                    case 'ZOC1':
                        ZOC1 = KWERT

                    # Joker
                    case 'ZJF1':
                        data['revenue']['joker_fee_gross'] = str(KWERT)
                    case 'ZJF2':
                        data['revenue']['joker_fee_net'] = str(KWERT)
                    # Customer Fee
                    case 'ZSFG':
                        data['revenue']['service_fee_gross'] = str(KWERT)
                    case 'ZSFN':
                        data['revenue']['service_fee_net'] = str(KWERT)

                    # Comission
                    case 'Z02N':
                        commission_standard['base'] = str(KWERT)
                    case 'ZCP2':
                        commission_standard['rate'] = str(KBETR)
                        commission_standard['amount'] = str(KWERT)
                        commission_standard['type'] = 'Standard'
                    case 'Z02T':
                        commission_tiers['base'] = str(KWERT)  # ?
                        commission_tiers['rate'] = str(KBETR)
                        commission_tiers['amount'] = str(KWERT)
                        commission_tiers['type'] = 'Tiers based commison'
                    case 'ZCP1':
                        commission_fixed['amount'] = str(KWERT)
                        commission_fixed['type'] = 'Fixed'
                    case 'MWST':
                        MWST = str(KWERT)
                    case 'Z04R':  # TW relevant only
                        Z04R = KWERT
                        data['vendor_refund']['gross_amount'] = str(
                            ZPR0 + Z04R)
                    case 'ZPR0':
                        if VBAK_AUART == 'ZAC0' & VBRP_NETWR < 0:
                            ZPR0 = KWERT
                            data['vendor_refund']['gross_amount'] = str(
                                ZPR0 + Z04R)
                            data['vendor_refund']['net_amount'] = str(
                                KWERT)
                            data['vendor_refund']['reason'] = VBAP_ARKTX
                        elif VBAK_AUART == 'ZAC0' & VBRP_NETWR > 0:
                            data['vendor_charges']['gross_amount'] = str(
                                KWERT)
                            data['vendor_charges']['net_amount'] = str(
                                KWERT)
                            data['vendor_charges']['reason'] = VBAP_ARKTX
                    # Tax total_amount
                    case 'ZVAM':
                        ZVAM = KWERT
                    case 'ZVA2':
                        ZVA2 = KWERT
                    case 'ZVA3':
                        ZVA3 = KWERT

            incentives_food_vendor = []
            incentives_food_platform = []
            incentives_delivery_vendor = []
            incentives_delivery_platform = []
            incentives_voucher_vendor = []
            incentives_voucher_platform = []

            if incentives_food_vendor_gross_amount is not None:
                incentives_food_vendor['gross_amount'] = incentives_food_vendor_gross_amount
                incentives_food_vendor['net_amount'] = incentives_food_vendor_net_amount
                incentives_food_vendor['type'] = 'food'
                incentives_food_vendor['owner'] = 'vendor'
                incentives_food_vendor['is_voucher'] = 'false'
                data['order_values']['incentives'].append(
                    incentives_food_vendor)

            if incentives_food_platform_gross_amount is not None:
                incentives_food_platform['gross_amount'] = incentives_food_platform_gross_amount
                incentives_food_platform['net_amount'] = incentives_food_platform_net_amount
                incentives_food_platform['type'] = 'food'
                incentives_food_platform['owner'] = 'vendor'
                incentives_food_platform['is_voucher'] = 'false'
                data['order_values']['incentives'].append(
                    incentives_food_platform)

            if incentives_delivery_vendor_gross_amount is not None:
                incentives_delivery_vendor['gross_amount'] = incentives_delivery_vendor_gross_amount
                incentives_delivery_vendor['net_amount'] = incentives_delivery_vendor_net_amount
                incentives_delivery_vendor['type'] = 'food'
                incentives_delivery_vendor['owner'] = 'vendor'
                incentives_delivery_vendor['is_voucher'] = 'false'
                data['order_values']['incentives'].append(
                    incentives_delivery_vendor)

            if incentives_delivery_platform_gross_amount is not None:
                incentives_delivery_platform['gross_amount'] = incentives_delivery_platform_gross_amount
                incentives_delivery_platform['net_amount'] = incentives_delivery_platform_net_amount
                incentives_delivery_platform['type'] = 'food'
                incentives_delivery_platform['owner'] = 'vendor'
                incentives_delivery_platform['is_voucher'] = 'false'
                data['order_values']['incentives'].append(
                    incentives_delivery_platform)

            if incentives_voucher_vendor_gross_amount is not None:
                incentives_voucher_vendor['gross_amount'] = incentives_voucher_vendor_gross_amount
                incentives_voucher_vendor['net_amount'] = incentives_voucher_vendor_net_amount
                incentives_voucher_vendor['type'] = 'food'
                incentives_voucher_vendor['owner'] = 'vendor'
                incentives_voucher_vendor['is_voucher'] = 'true'
                data['order_values']['incentives'].append(
                    incentives_voucher_vendor)

            if incentives_voucher_platform_gross_amount is not None:
                incentives_voucher_platform['gross_amount'] = incentives_voucher_platform_gross_amount
                incentives_voucher_platform['net_amount'] = incentives_voucher_platform_net_amount
                incentives_voucher_platform['type'] = 'food'
                incentives_voucher_platform['owner'] = 'vendor'
                incentives_voucher_platform['is_voucher'] = 'true'
                data['order_values']['incentives'].append(
                    incentives_voucher_platform)

            data['order_values']['total_order_value_gross'] = data['customer_paid_amount']
            data['order_values']['total_order_value_net'] = 'TODO'

            data['order_values']['total_tax_value'] = str(ZVAM + ZVA2 + ZVA3)
            data['order_values']['tax_inclusive_amount'] = ''
            data['order_values']['tax_exclusive_amount'] = ''

            data['revenue']['payment_fee_net'] = ZOC2 + ZOC1
            data['revenue']['payment_fee_gross'] = data['revenue']['payment_fee_net'] * \
                (1 + MWST)

            data['revenue']['payment_provider'] = row[5]

            data['revenue']['vendor_funded_delivery_fee_incentive_gross'] = 'TODO'
            data['revenue']['vendor_funded_delivery_fee_incentive_net'] = 'TODO'

            # TODO
            data['revenue']['commissions'] = []

            data['revenue']['tax_charge'] = str(row[8])

            if 'gross_delivery_amount' in discount_vendor:
                discount_vendor['owner'] = 'Vendor'
                data['discount'].append(discount_vendor)

            if 'gross_delivery_amount' in discount_plaform:
                discount_vendor['owner'] = 'Platform'
                data['discount'].append(discount_plaform)

            if 'amount' in commission_standard:
                data['commission'].append(commission_standard)

            if 'amount' in commission_fixed:
                data['commission'].append(commission_fixed)

            if 'amount' in commission_tiers:
                data['commission'].append(commission_tiers)

            if 'net_amount' in data['vendor_refund']:
                data['vendor_refund']['gross_amount'] = str(
                    float(data['vendor_refund']['gross_amount']) + MWST)

            if 'net_amount' in data['vendor_charges']:
                data['vendor_charges']['gross_amount'] = str(
                    float(data['vendor_charges']['gross_amount']) + MWST)

            json_data = json.dumps(data, sort_keys=False, indent=4)
            f = open("output/" + global_entity_id +
                     "_" + order_id[0] + ".json", "w+")
            f.write(json_data)
            f.close()
            print(global_entity_id + " " + order_id[0] + ' done!')
    except:
        logging.exception('HANA DB connection error')


main_program()

# SE
# pvyg-unj3
# xyke-a66l
# v3ea-6w4z
# v7mi-6vvi
# r1f7-tht6


# TW
# d4ja-2hvf
# d4ja-rqjo
