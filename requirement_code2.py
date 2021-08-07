import json, math, psycopg2
from pymongo import MongoClient
from datetime import datetime, timedelta
import requests
import boto3
import time
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime as dt

def db_credential(db_name):
    url = "http://ec2-13-234-21-229.ap-south-1.compute.amazonaws.com/db_credentials/"

    payload = json.dumps({
        "data_base_name": db_name
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = dict(requests.post(url, data=payload, headers=headers).json())
    print("response===>", response)
    status = response['status']
    print("payload", payload)
    if status == True:
        return {

            'response': response
        }
    else:
        return {
            'response': response
        }


# def lambda_handler(event, context):
def lambda_handler():
    try:
        ###sqlachemy connection
        db_name = 'postgres'
        credential = db_credential(db_name)
        print("credential====", credential)
        cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
        print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
        ##orders
        cred_for_sqlalchemy_orders = cred_for_sqlalchemy + "/orders"
        print("cred_for_sqlalchemy_orders--", cred_for_sqlalchemy_orders)
        ##products
        cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
        print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)
        ##warehouse
        cred_for_sqlalchemy_warehouse = cred_for_sqlalchemy + "/warehouse"
        print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)
        ###psycopg2 connection
        cred_for_psycopg2 = credential["response"]["db_detail"]["db_detail_for_psycopg2"]
        print("cred_for_psycopg2--", cred_for_psycopg2)
        rds_host = cred_for_psycopg2['endPoint']
        name = cred_for_psycopg2['userName']
        password = cred_for_psycopg2['passWord']
        ##orders
        db_name_orders = "orders"
        ##products
        db_name_products = "products"
        ##users
        db_name_users = "users"
        ##warehouse
        db_name_warehouse = "warehouse"
        message = "Nothing to run"
        statusCode = 200
        start_time = time.time()
        conn_products = psycopg2.connect(host=rds_host, database=db_name_products, user=name, password=password)
        conn_orders = psycopg2.connect(host=rds_host, database=db_name_orders, user=name, password=password)
        conn_warehouse = psycopg2.connect(host=rds_host, database=db_name_warehouse, user=name, password=password)
        sqs = boto3.client('sqs')
        cursor_product = conn_products.cursor()
        cursor_orders = conn_orders.cursor()
        cursor_warehouse = conn_warehouse.cursor()

        # FOR MONGO
        client = MongoClient(
            'mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/wms?retryWrites=true&w=majority')
        db = client.wms
        # bin_reco_count = db.api_binrecocount
        ###for buymore_sku concept
        requirement_2 = db.requirement_2
        ###testing input method

        #         if event.get("Records",None) is not None:
        # 			data = json.loads(event["Records"][0]["body"])
        # 		else:
        # 			data = event
        # 		#print(data)

        # 		product_id_list = data.get('product_id_list',None)
        ###for sqs trigger
        # print("event==", event)
        # data = json.loads(event['Records'][0]["body"])
        # print("data===", data)
        # product_id_list = data.get('product_id_list', None)
        # print("product_id_list===>", product_id_list)
        ###for manual lambda trigger
        # product_id_list = event.get("product_id_list", None)
        # print("product_id_list===>", product_id_list)
        product_id_list = ["235740","112363"]
        # product_id_list = ["235740"]

        productQuery = "SELECT ccr.category_id,aap.amazon_listing_id,\
																			 mm.product_id, mm.buymore_sku,\
																			 ccr.a, ccr.b, \
																			 ccr.c, ccr.d, \
																			 mm.child_variations, mm.sales_rank \
																			 FROM public.calculation_categoryrequirement AS ccr\
																			 LEFT JOIN public.master_masterproduct AS mm \
																			 ON ccr.category_id = mm.category_id_id \
																			 LEFT JOIN public.amazon_amazonproducts AS aap\
																			 ON mm.product_id = aap.product_id \
																			 WHERE mm.product_id "

        if product_id_list is not None:
            priduct_list = ",".join(product_id_list)
            productQuery = productQuery + " in (" + priduct_list + ")"

        if len(productQuery) != 0:

            cursor_product.execute(productQuery)
            res = cursor_product.fetchall()
            # print("res===", res)
            ##to get column names from the data
            column_names = [desc[0] for desc in cursor_product.description]
            # print("column_names==", column_names)
            results_data = []

            if res is not None:
                for single_record in res:
                    results_data.append(dict(zip(column_names, single_record)))
            # print("results_data===", results_data)
            ###create a empty data frame
            data = pd.DataFrame(columns=['product_id_id', '30_qty_north_zone', '30_qty_east_zone', '30_qty_south_zone',
                                         '30_qty_west_zone',
                                         '90_qty_north_zone', '90_qty_east_zone', '90_qty_south_zone',
                                         '90_qty_west_zone', 'north_stock', 'east_stock', 'south_stock',
                                         'requirement_value', 'west_stock', 'wms_stock'])
            data.to_csv('z20.csv', index=False)
            data.to_csv('z21.csv', index=False)
            for single_record in results_data:

                d = pd.read_csv('z21.csv')

                # after every itration delete all rows
                d.iloc[0:0]
                p = single_record['product_id']
                print("product_id==", p)
                d.at[0, 'product_id_id'] = p

                try:

                    today = datetime.now()
                    # print("today==", today)
                    todayDate = today.strftime("%Y-%m-%d")
                    # print("todayDate==", todayDate)

                    ###for 30 days
                    thirty_days_orders = 0
                    thirty_days = today + timedelta(days=-30)
                    # print("thirty_days==", thirty_days)
                    thirty_daysDate = thirty_days.strftime("%Y-%m-%d")
                    # print("thirty_daysDate==", thirty_daysDate)

                    ###for 90 days
                    ninty_days_orders = 0
                    todayDate = today.strftime("%Y-%m-%d")
                    # print("todayDate==", todayDate)
                    ninty_days = today + timedelta(days=-90)
                    # print("ninty_days==", ninty_days)
                    ninty_days_Date = ninty_days.strftime("%Y-%m-%d")
                    # print("ninty_days_Date==", ninty_days_Date)
                    ##for dynamic warehouse id creation
                    ##for reference use state_zone_map1
                    # state_zone_map1 = {"AP": 2, "AR ": 3, "AS": 3, "BR": 1, "CG": 1, "GA": 4, "GJ": 4, "HR": 1, "HP": 3,
                    #                   "JK": 3, "JH": 1, "KA": 2, "KL": 2, "MP": 1, "MH": 4, "MN": 3, "ML": 3, "MZ": 3,
                    #                   "NL": 3, "OR": 2, "PB": 2, "RJ": 1, "SK": 3, "TN": 2, "TS": 2, "TR": 3, "UP": 1,
                    #                   "UK": 1, "WB": 3, "AN": 2, "CH": 1, "DH": 3, "DD": 2, "LD": 2, "NCR": 1, "PY": 2}
                    state_zone_map = {
                        'state': ["AP", "AR ", "AS", "BR", "CG", "GA", "GJ", "HR", "HP",
                                  "JK", "JH", "KA", "KL", "MP", "MH", "MN", "ML", "MZ",
                                  "NL", "OR", "PB", "RJ", "SK", "TN", "TS", "TR", "UP",
                                  "UK", "WB", "AN", "CH", "DH", "DD", "LD", "NCR", "PY", "DL"],
                        'id': [2, 3, 3, 1, 1, 4, 4, 1, 3,
                               3, 1, 2, 2, 1, 4, 3, 3, 3,
                               3, 2, 2, 1, 3, 2, 2, 3, 1,
                               1, 3, 2, 1, 3, 2, 2, 1, 2, 1]
                    }

                    # creating a Dataframe object
                    zone = pd.DataFrame(state_zone_map)
                    # print(zone)
                    zone.to_csv('z1.csv', index=False)
                    engine = create_engine(cred_for_sqlalchemy_warehouse)
                    query = 'select id,state from api_warehousedetails'
                    sql = pd.read_sql(query, engine)
                    # print("sql==",sql)
                    sql['zone'] = 0
                    sql.to_csv('z.csv', index=False)

                    df115 = pd.read_csv("z.csv")
                    df125 = pd.read_csv("z1.csv")
                    df225 = pd.merge(left=df115, right=df125, left_on='state', right_on='state')
                    df225['zone'] = df225['id_y']
                    df2252 = df225.drop(['id_y'], axis=1)
                    df2252.rename(columns={'id_x': 'id'}, inplace=True)
                    df2252['zone'] = df2252['zone'].apply(str)
                    df2252.to_csv('z2.csv', index=False)

                    ## warehouse belongs to north zone code ==2
                    wb = pd.read_csv('z2.csv')
                    north = wb[wb['zone'] == 1]
                    north_wb = north['id'].astype(str).values.tolist()
                    # north_wb=north['id'].tolist()
                    north_wb1 = str(north_wb)
                    n = north_wb1[1:-1]
                    ##for wms
                    # print(">north_wb=====", north_wb)
                    n_wb_list = [int(i) for i in north_wb]
                    # print("n_wb_list==", n_wb_list)
                    n2 = n_wb_list
                    # print("n2==", n2)

                    # print("north_wb1===", north_wb1[1:-1])
                    if north_wb is not None:
                        north_wb_list = north_wb
                        # print("north_wb_list===", north_wb_list)
                        str1 = ''.join(north_wb_list)
                        # print("str1===", str1)
                        # north_wb_Query = " in (" + north_wb_list[1:-1] + ")"
                        north_wb_Query = " in (" + n + ")"
                    else:
                        north_wb_Query = " in ('0')"
                    # print("north_wb_Query===", north_wb_Query)

                    # warehouse belongs to east zone code ==3
                    wb1 = pd.read_csv('z2.csv')
                    east = wb1[wb1['zone'] == 3]
                    east_wb = east['id'].astype(str).values.tolist()
                    # print("east_wb===", east_wb)
                    east_wb1 = str(east_wb)
                    e = east_wb1[1:-1]
                    ##for wms
                    # print(">east_wb=====", east_wb)
                    e_wb_list = [int(i) for i in east_wb]
                    # print("e_wb_list==", e_wb_list)
                    e2 = e_wb_list
                    # print("e2==", e2)
                    if east_wb is not None:
                        # east_wb_list = ",".join(east_wb)
                        east_wb_Query = " in (" + e + ")"
                    else:
                        east_wb_Query = " in ('0')"
                    # print("east_wb_Query===", east_wb_Query)

                    # warehouse belongs to south zone
                    wb2 = pd.read_csv('z2.csv')
                    south = wb2[wb2['zone'] == 2]
                    south_wb = south['id'].astype(str).values.tolist()
                    # print("south_wb===", south_wb)
                    south_wb1 = str(south_wb)
                    s = south_wb1[1:-1]
                    ##for wms
                    # print(">south_wb=====", south_wb)
                    s_wb_list = [int(i) for i in south_wb]
                    # print("s_wb_list==", s_wb_list)
                    s2 = s_wb_list
                    # print("s2==", s2)
                    if south_wb is not None:
                        # south_wb_list = ",".join(south_wb)
                        south_wb_Query = " in (" + s + ")"
                    else:
                        south_wb_Query = " in ('0')"
                    # print("south_wb_Query===", south_wb_Query)

                    # warehouse belongs to west zone
                    wb3 = pd.read_csv('z2.csv')
                    west = wb3[wb3['zone'] == 4]
                    west_wb = west['id'].astype(str).values.tolist()
                    # print("west_wb===", west_wb)
                    west_wb1 = str(west_wb)
                    w = west_wb1[1:-1]
                    ##for wms
                    # print(">west_wb=====", west_wb)
                    w_wb_list = [int(i) for i in west_wb]
                    # print("w_wb_list==", w_wb_list)
                    w2 = w_wb_list
                    # print("w2==", w2)
                    if west_wb is not None:
                        # west_wb_list = ",".join(west_wb)
                        west_wb_Query = " in (" + w + ")"
                    else:
                        west_wb_Query = " in ('0')"
                    # print("west_wb_Query===", west_wb_Query)

                    ###30 days query per zone wise
                    ##North Zone
                    orders_query1 = "SELECT SUM(ano.qty) from public.api_neworder AS ano WHERE ano.product_id=" + str(
                        single_record[
                            'product_id']) + " and date(ano.order_date) BETWEEN '{thirty_daysDate}' AND '{todayDate}'".format(
                        todayDate=todayDate, thirty_daysDate=thirty_daysDate) + ' and warehouse_id ' + north_wb_Query
                    # print("orders_query1===", orders_query1)
                    cursor_orders.execute(orders_query1)
                    n_res1 = cursor_orders.fetchone()
                    # print("n_res1==>>", n_res1[0])
                    d['30_qty_north_zone'] = n_res1[0]

                    ##East Zone
                    orders_query2 = "SELECT SUM(ano.qty) from public.api_neworder AS ano WHERE ano.product_id=" + str(
                        single_record[
                            'product_id']) + " and date(ano.order_date) BETWEEN '{thirty_daysDate}' AND '{todayDate}'".format(
                        todayDate=todayDate, thirty_daysDate=thirty_daysDate) + ' and warehouse_id ' + east_wb_Query
                    # print("orders_query2===", orders_query2)
                    cursor_orders.execute(orders_query2)
                    e_res1 = cursor_orders.fetchone()
                    # print("e_res1==>>", e_res1[0])
                    d['30_qty_east_zone'] = e_res1[0]
                    ##South Zone
                    orders_query3 = "SELECT SUM(ano.qty) from public.api_neworder AS ano WHERE ano.product_id=" + str(
                        single_record[
                            'product_id']) + " and date(ano.order_date) BETWEEN '{thirty_daysDate}' AND '{todayDate}'".format(
                        todayDate=todayDate, thirty_daysDate=thirty_daysDate) + ' and warehouse_id ' + south_wb_Query
                    # print("orders_query3===", orders_query3)
                    cursor_orders.execute(orders_query3)
                    s_res1 = cursor_orders.fetchone()
                    # print("s_res1==>>", s_res1[0])
                    d['30_qty_south_zone'] = s_res1[0]
                    ##West Zone
                    orders_query4 = "SELECT SUM(ano.qty) from public.api_neworder AS ano WHERE ano.product_id=" + str(
                        single_record[
                            'product_id']) + " and date(ano.order_date) BETWEEN '{thirty_daysDate}' AND '{todayDate}'".format(
                        todayDate=todayDate, thirty_daysDate=thirty_daysDate) + ' and warehouse_id ' + west_wb_Query
                    # print("orders_query4===", orders_query4)
                    cursor_orders.execute(orders_query4)
                    w_res1 = cursor_orders.fetchone()
                    # print("w_res1==>>", w_res1[0])
                    d['30_qty_west_zone'] = w_res1[0]

                    ###90 days query per zone wise
                    ##North Zone
                    orders_query91 = "SELECT SUM(ano.qty) from public.api_neworder AS ano WHERE ano.product_id=" + str(
                        single_record[
                            'product_id']) + " and date(ano.order_date) BETWEEN '{ninty_daysDate}' AND '{todayDate}'".format(
                        todayDate=todayDate, ninty_daysDate=ninty_days_Date) + ' and warehouse_id ' + north_wb_Query
                    # print("orders_query1===", orders_query91)
                    cursor_orders.execute(orders_query91)
                    n_res91 = cursor_orders.fetchone()
                    # print("n_res91==>>", n_res91[0])
                    d['90_qty_north_zone'] = n_res91[0]
                    ##East Zone
                    orders_query92 = "SELECT SUM(ano.qty) from public.api_neworder AS ano WHERE ano.product_id=" + str(
                        single_record[
                            'product_id']) + " and date(ano.order_date) BETWEEN '{ninty_daysDate}' AND '{todayDate}'".format(
                        todayDate=todayDate, ninty_daysDate=ninty_days_Date) + ' and warehouse_id ' + east_wb_Query
                    # print("orders_query2===", orders_query92)
                    cursor_orders.execute(orders_query92)
                    e_res92 = cursor_orders.fetchone()
                    # print("e_res92==>>", e_res92[0])
                    d['90_qty_east_zone'] = e_res92[0]
                    ##South Zone
                    orders_query93 = "SELECT SUM(ano.qty) from public.api_neworder AS ano WHERE ano.product_id=" + str(
                        single_record[
                            'product_id']) + " and date(ano.order_date) BETWEEN '{ninty_daysDate}' AND '{todayDate}'".format(
                        todayDate=todayDate, ninty_daysDate=ninty_days_Date) + ' and warehouse_id ' + south_wb_Query
                    # print("orders_query3===", orders_query93)
                    cursor_orders.execute(orders_query93)
                    s_res93 = cursor_orders.fetchone()
                    # print("s_res93==>>", s_res93[0])
                    d['90_qty_south_zone'] = s_res93[0]
                    ##West Zone
                    orders_query94 = "SELECT SUM(ano.qty) from public.api_neworder AS ano WHERE ano.product_id=" + str(
                        single_record[
                            'product_id']) + " and date(ano.order_date) BETWEEN '{ninty_daysDate}' AND '{todayDate}'".format(
                        todayDate=todayDate, ninty_daysDate=ninty_days_Date) + ' and warehouse_id ' + west_wb_Query
                    # print("orders_query4===", orders_query94)
                    cursor_orders.execute(orders_query94)
                    w_res94 = cursor_orders.fetchone()
                    # print("w_res94==>>", w_res94[0])
                    d['90_qty_west_zone'] = w_res94[0]
                    d.to_csv('z30.csv', index=False)

                except:
                    pass
                wms_stock = 0
                north_stock = 0  # id 1
                east_stock = 0  # id 3
                south_stock = 0  # id 2
                west_stock = 0  # id 4
                # amazon_listing_id_1=single_record['amazon_listing_id']
                ###for buymore_sku concept
                amazon_listing_id_1 = single_record['buymore_sku']
                if amazon_listing_id_1 is None:
                    # print("pass===>")
                    pass
                else:

                    # wms_data = bin_reco_count.find({"_id.fnsku": single_record['amazon_listing_id']})
                    ###for buymore_sku concept
                    wms_data = requirement_2.find({"_id.buymore_sku": single_record['buymore_sku']})
                    # print(wms_data)

                    if wms_data is not None:
                        # print("inside wms")
                        for wms_single in list(wms_data):
                            # print(wms_single)

                            for n in n2:
                                if int(wms_single['_id']['warehouseId']) == n:
                                    north_stock += int(wms_single["total"])

                                else:
                                    pass

                            # elif int(wms_single['_id']['warehouseId']) == e2:
                            for e in e2:
                                if int(wms_single['_id']['warehouseId']) == e:
                                    east_stock += int(wms_single["total"])

                                else:
                                    pass

                            # elif int(wms_single['_id']['warehouseId']) == s2:
                            for s in s2:
                                if int(wms_single['_id']['warehouseId']) == s:
                                    south_stock += int(wms_single["total"])

                                else:
                                    pass

                            # elif int(wms_single['_id']['warehouseId']) == w2:
                            for w in w2:
                                if int(wms_single['_id']['warehouseId']) == w:
                                    west_stock += int(wms_single["total"])

                                else:
                                    pass

                            wms_stock += int(wms_single["total"])
                # print("north_stock===", north_stock)
                # print("east_stock===", east_stock)
                # print("south_stock===", south_stock)
                # print("west_stock===", west_stock)
                # print("wms_stock===", wms_stock)
                d['north_stock'] = north_stock
                d['east_stock'] = east_stock
                d['south_stock'] = south_stock
                d['west_stock'] = west_stock
                d['wms_stock'] = wms_stock
                #
                new_child_value = min(single_record['child_variations'], 20)
                # print(single_record)
                ###req_value modification
                req_value = (single_record['a'] * (pow(single_record['sales_rank'], -(single_record['b'] + (
                        single_record['c'] * pow(math.log10(single_record['sales_rank']), 2)) + (
                                                                                              single_record['d'] * (
                                                                                          math.log10(single_record[
                                                                                                         'sales_rank']))))))) / new_child_value
                # print("req_value==", req_value)
                # print("req_value===", req_value)
                d['requirement_value'] = req_value
                ##to convert exponent value to integer value

                ###to fill all null or empty value with 0
                d.fillna(value=0, inplace=True)
                ###if wms stock value less than 0 or negative take is as 0
                cols = ['north_stock', 'east_stock', 'south_stock', 'west_stock']
                d[d[cols] < 0] = 0

                d.to_csv('z19.csv', index=False)
                final = pd.read_csv('z19.csv')
                final['a'] = 0.15 * (
                        final['30_qty_north_zone'] + final['30_qty_east_zone'] + final['30_qty_south_zone'] + final[
                    '30_qty_west_zone'])
                final['b'] = 0.15 * (
                        final['90_qty_north_zone'] + final['90_qty_east_zone'] + final['90_qty_south_zone'] + final[
                    '90_qty_west_zone'])
                final['a'] = final['a'].apply(lambda x: round(x, 2))
                final['b'] = final['b'].apply(lambda x: round(x, 2))

                final['north_zone_requirement'] = 0
                final['east_zone_requirement'] = 0
                final['south_zone_requirement'] = 0
                final['west_zone_requirement'] = 0
                final['north_zone_excess_stock'] = 0
                final['east_zone_excess_stock'] = 0
                final['south_zone_excess_stock'] = 0
                final['west_zone_excess_stock'] = 0
                final['created_at'] = ''
                final['updated_at'] = ''

                final.to_csv('z22.csv', index=False)
                final1 = pd.read_csv('z22.csv')
                final1.created_at.fillna(dt.datetime.now(), inplace=True)
                final1.updated_at.fillna(dt.datetime.now(), inplace=True)
                final1.to_csv('z23.csv', index=False)
                final2 = pd.read_csv('z23.csv')
                final2['30_qty_north_zone1'] = final2['30_qty_north_zone'] * 3
                final2['30_qty_east_zone1'] = final2['30_qty_east_zone'] * 3
                final2['30_qty_south_zone1'] = final2['30_qty_south_zone'] * 3
                final2['30_qty_west_zone1'] = final2['30_qty_west_zone'] * 3
                final2['north_zone_requirement'] = final2[
                    ["30_qty_north_zone1", "90_qty_north_zone", "requirement_value", "a", "b"]].max(axis=1)
                # print("1===", final2['north_zone_requirement'])
                final2['east_zone_requirement'] = final2[
                    ["30_qty_east_zone1", "90_qty_east_zone", "requirement_value", "a", "b"]].max(axis=1)
                # print("2===", final2['east_zone_requirement'])
                final2['south_zone_requirement'] = final2[
                    ["30_qty_south_zone1", "90_qty_south_zone", "requirement_value", "a", "b"]].max(axis=1)
                # print("3===", final2['south_zone_requirement'])
                final2['west_zone_requirement'] = final2[
                    ["30_qty_west_zone1", "90_qty_west_zone", "requirement_value", "a", "b"]].max(axis=1)
                # print("4===", final2['west_zone_requirement'])
                final2.to_csv('z24.csv', index=False)
                final4 = pd.read_csv('z24.csv')
                # ###to fill all null or empty value with 0

                final4.to_csv('z25.csv', index=False)
                final5 = pd.read_csv('z25.csv')
                ###for renaming column name as per db
                final5 = final5.rename(columns={'north_stock': 'north_zone_stock'})
                final5 = final5.rename(columns={'east_stock': 'east_zone_stock'})
                final5 = final5.rename(columns={'south_stock': 'south_zone_stock'})
                final5 = final5.rename(columns={'west_stock': 'west_zone_stock'})
                ###30 days
                final5 = final5.rename(columns={'30_qty_north_zone': 'last_month_sale_qty_north_zone'})
                final5 = final5.rename(columns={'30_qty_east_zone': 'last_month_sale_qty_east_zone'})
                final5 = final5.rename(columns={'30_qty_south_zone': 'last_month_sale_qty_south_zone'})
                final5 = final5.rename(columns={'30_qty_west_zone': 'last_month_sale_qty_west_zone'})
                ###90 days
                final5 = final5.rename(columns={'90_qty_north_zone': 'last_3_month_sale_qty_north_zone'})
                final5 = final5.rename(columns={'90_qty_east_zone': 'last_3_month_sale_qty_east_zone'})
                final5 = final5.rename(columns={'90_qty_south_zone': 'last_3_month_sale_qty_south_zone'})
                final5 = final5.rename(columns={'90_qty_west_zone': 'last_3_month_sale_qty_west_zone'})
                ###for days_of_cover field
                p1 = single_record['product_id']
                # p=final5['product_id_id'][0]
                # print("p-----------",p1)
                engine1 = create_engine(cred_for_sqlalchemy_products)
                query2 = "select days_of_cover from master_stockrequirement_1 where product_id_id = " + str(p1)
                # print("query2==",query2)
                sqlp = pd.read_sql(query2, engine1)
                if sqlp.empty:
                    final5['days_of_cover'] = 30
                else:
                    days_of_cover0 = sqlp["days_of_cover"][0]
                    # print("days_of_cover0===", days_of_cover0)
                    final5['days_of_cover'] = days_of_cover0

                final5.drop(['wms_stock', 'a', 'b'], axis=1, inplace=True)
                # final5['stock_requirement_id'] = 6

                # cols1 = ['north_zone_excess_stock','east_zone_excess_stock','south_zone_excess_stock','west_zone_excess_stock']
                # final5[final5[cols1] < 0] = 0
                final51 = final5.drop(
                    ['30_qty_north_zone1', '30_qty_east_zone1', '30_qty_south_zone1', '30_qty_west_zone1'], axis=1)
                final51['north_zone_requirement'] = ((final51['north_zone_requirement'] * final51['days_of_cover']) / 90) - final51['north_zone_stock']
                final51['east_zone_requirement'] = ((final51['east_zone_requirement'] * final51['days_of_cover']) / 90) - final51['east_zone_stock']
                final51['south_zone_requirement'] = ((final51['south_zone_requirement'] * final51['days_of_cover']) / 90) - final51['south_zone_stock']
                final51['west_zone_requirement'] = ((final51['west_zone_requirement'] * final51['days_of_cover']) / 90) - final51['west_zone_stock']
                ###if wms requirement value less than 0 or negative take is as 0
                final51['north_zone_requirement'] = final51['north_zone_requirement'].apply(
                    lambda x: x if x > 0 else 0)
                final51['east_zone_requirement'] = final51['east_zone_requirement'].apply(
                    lambda x: x if x > 0 else 0)
                final51['south_zone_requirement'] = final51['south_zone_requirement'].apply(
                    lambda x: x if x > 0 else 0)
                final51['west_zone_requirement'] = final51['west_zone_requirement'].apply(
                    lambda x: x if x > 0 else 0)
                ###added code
                final51['north_zone_excess_stock'] = final51['north_zone_stock'] - final51['north_zone_requirement']
                final51['east_zone_excess_stock'] = final51['east_zone_stock'] - final51['east_zone_requirement']
                final51['south_zone_excess_stock'] = final51['south_zone_stock'] - final51['south_zone_requirement']
                final51['west_zone_excess_stock'] = final51['west_zone_stock'] - final51['west_zone_requirement']

                ###if wms stock value less than 0 or negative take is as 0
                final51['north_zone_excess_stock'] = final51['north_zone_excess_stock'].apply(
                    lambda x: x if x > 0 else 0)
                final51['east_zone_excess_stock'] = final51['east_zone_excess_stock'].apply(lambda x: x if x > 0 else 0)
                final51['south_zone_excess_stock'] = final51['south_zone_excess_stock'].apply(
                    lambda x: x if x > 0 else 0)
                final51['west_zone_excess_stock'] = final51['west_zone_excess_stock'].apply(lambda x: x if x > 0 else 0)
                final51['north_zone_excess_stock'] = final51['north_zone_excess_stock'].apply(lambda x: round(x, 2))
                final51['east_zone_excess_stock'] = final51['east_zone_excess_stock'].apply(lambda x: round(x, 2))
                final51['south_zone_excess_stock'] = final51['south_zone_excess_stock'].apply(lambda x: round(x, 2))
                final51['west_zone_excess_stock'] = final51['west_zone_excess_stock'].apply(lambda x: round(x, 2))
                final51.to_csv('z26.csv', index=False)

                ## for validation

                # engine1 = create_engine(
                #     'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/products')
                query1 = 'select product_id_id from master_stockrequirement_1'
                sql12 = pd.read_sql(query1, engine1)
                product_id_list = sql12["product_id_id"].tolist()
                # print("product_id_list===", product_id_list)
                ### for new data
                try:
                    ###data not in db then have to insert

                    df12 = pd.read_csv('z26.csv')
                    dd1 = df12[~df12['product_id_id'].isin(product_id_list)]
                    dd1.to_csv('z31.csv', index=False)

                except:
                    pass
                #####for already present data data
                try:
                    ######data in db then have to update
                    df11 = pd.read_csv('z26.csv')
                    dd = df11[df11['product_id_id'].isin(product_id_list)]
                    dd.to_csv('z30.csv', index=False)


                except:
                    pass

                ##insert data
                if dd1.empty:
                    pass
                else:
                    # print("data inserting")
                    engine1 = create_engine(cred_for_sqlalchemy_products)
                    dd1.to_sql(
                        name='master_stockrequirement_1',
                        con=engine1,
                        index=False,
                        if_exists='append'
                    )
                ###update data
                if dd.empty:
                    pass
                else:
                    iters = dd.iterrows()
                    # print("data updating")
                    ##mod7

                    for index, row in iters:
                        # rds_host1 = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
                        # name1 = "postgres"
                        # password1 = "buymore2"
                        # ##mod1
                        # db_name1 = "products"
                        conn1 = psycopg2.connect(host=rds_host,
                                    database=db_name_products,
                                    user=name,
                                    password=password)
                        cur1 = conn1.cursor()

                        # cur1.execute(
                        #     'UPDATE "master_stockrequirement_1" SET "days_of_cover" = %s,"north_zone_stock"= %s,"south_zone_stock"= %s,"east_zone_stock"= %s,"west_zone_stock"= %s,"last_month_sale_qty_north_zone"= %s,"last_month_sale_qty_south_zone"= %s,"last_month_sale_qty_east_zone"= %s,"last_month_sale_qty_west_zone"= %s,"last_3_month_sale_qty_north_zone"= %s,"last_3_month_sale_qty_south_zone"= %s,"last_3_month_sale_qty_east_zone"= %s,"last_3_month_sale_qty_west_zone"= %s,"requirement_value"= %s,"north_zone_requirement"= %s,"south_zone_requirement"= %s,"east_zone_requirement"= %s,"west_zone_requirement"= %s,"north_zone_excess_stock"= %s,"south_zone_excess_stock"= %s,"east_zone_excess_stock"= %s,"west_zone_excess_stock"= %s,"updated_at"= %s WHERE "master_stockrequirement_1"."product_id_id" = %s',
                        #     [row['days_of_cover'], row['north_zone_stock'],
                        #      row['south_zone_stock'],
                        #      row['east_zone_stock'],
                        #      row['west_zone_stock'],
                        #      row['last_month_sale_qty_north_zone'], row['last_month_sale_qty_south_zone'],
                        #      row['last_month_sale_qty_east_zone'],
                        #      row['last_month_sale_qty_west_zone'],
                        #      row['last_3_month_sale_qty_north_zone'], row['last_3_month_sale_qty_south_zone'],
                        #      row['last_3_month_sale_qty_east_zone'], row['last_3_month_sale_qty_west_zone'],
                        #      row['requirement_value'], row['north_zone_requirement'], row['south_zone_requirement'],
                        #      row['east_zone_requirement'],
                        #      row['west_zone_requirement'],
                        #      row['north_zone_excess_stock'], row['south_zone_excess_stock'],
                        #      row['east_zone_excess_stock'],
                        #      row['west_zone_excess_stock'], row['updated_at'],
                        #      row['product_id_id']])
                        # conn1.commit()
                        # conn1.close()
                ##to delete rows from file after one iteration
                d11 = pd.read_csv('z26.csv')
                d22 = d11.iloc[0:0]
                # after every itration delete all rows
                d22.to_csv('z26.csv', index=False)

            engine.dispose()
            engine1.dispose()
            return {
                'statusCode': 200,
                'Message': "data inserted successful"

            }

    except Exception as e:
        message = "Exception --" + str(e)
        return {
            'statusCode': 400,
            'Message': "File upload error",
            'error': {e}

        }
    finally:
        conn_products.close()
        conn_orders.close()
        cursor_product.close()
        cursor_orders.close()

        a = statusCode
        print("statusCode", a)

print(lambda_handler())