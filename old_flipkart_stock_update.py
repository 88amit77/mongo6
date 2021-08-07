import json
from pymongo import MongoClient
import csv
from datetime import datetime
import psycopg2
import requests
import os
import dropbox
import sys
import re
from sqlalchemy import create_engine
import pandas as pd
##db settings
def db_credential(db_name):
    url = "http://ec2-13-234-21-229.ap-south-1.compute.amazonaws.com/db_credentials/"

    payload = json.dumps({
        "data_base_name": db_name
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = dict(requests.post(url, data=payload, headers=headers).json())
    # print("response===>", response)
    status = response['status']
    # print("payload", payload)
    if status == True:
        return {

            'response': response
        }
    else:
        return {
            'response': response
        }
###sqlachemy connection
db_name = 'postgres'
credential = db_credential(db_name)
# print("credential====", credential)
cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
# print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
##orders
cred_for_sqlalchemy_orders = cred_for_sqlalchemy + "/orders"
# print("cred_for_sqlalchemy_orders--", cred_for_sqlalchemy_orders)
##products
cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
# print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)
###psycopg2 connection
cred_for_psycopg2 = credential["response"]["db_detail"]["db_detail_for_psycopg2"]
# print("cred_for_psycopg2--", cred_for_psycopg2)
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
##vendors
db_name_vendors = "vendors"
##employees
db_name_employees = "employees"

# Add Portal related Functions

#flipkart portal functions
def get_flipkart_accounts(cur_products):
    print('fetch accounts')
    accounts = []
    cur_products.execute("Select * from master_portalaccount where portal_name = 'Flipkart'")
    portal = cur_products.fetchone()
    if portal is not None:
        portal_id = portal[0]
        cur_products.execute("Select * from master_portalaccountdetails where portal_id_id = " + str(portal_id))
        portal_accounts = cur_products.fetchall()
        if len(portal_accounts):
            for portal_account in portal_accounts:
                access_token = ''
                access_token_field = portal_account[4]
                for item in access_token_field:
                    if item['name'] == 'access token':
                        access_token = item['base_url']
                        break
                warehouses = []
                warehouse_field = portal_account[5]
                for item in warehouse_field:
                    warehouse = {
                        'warehouse_id': item['warehouse_id'],
                        'location_id': item['portal_warehouse']
                    }
                    warehouses.append(warehouse)
                account = {
                    "app_id": "14808635b618aa6b7316b77461a04356a460b",
                    "app_secret": "3dcf741f1de2edbc523c01c067497262",
                    "account_name": portal_account[1],
                    "account_id": portal_account[0],
                    "warehouses": warehouses,
                    "access_token": access_token,
                    "portal_id": portal_id
                }
                accounts.append(account)
    return accounts


def get_location_id(account, warehouse_id):
    warehouses = account['warehouses']
    for warehouse in warehouses:
        if warehouse_id == warehouse['warehouse_id']:
            return warehouse['location_id']


def get_flipkart_product(product_id, cur_products):
    product_query = "SELECT flipkart_portal_sku, flipkart_portal_unique_id, flipkart_account_id from " \
                    "flipkart_flipkartproducts where product_id = " + str(product_id)
    cur_products.execute(product_query)
    product = cur_products.fetchone()
    if product is not None:
        return {
            'sku': product[0],
            'unique_id': product[1].replace('\n', ''),
            'account_id': product[2]
        }
    else:
        return None


def update_flipkart_inventory(access_token, set):
    url = 'https://api.flipkart.net/sellers/listings/v3/update/inventory'
    response = dict(requests.post(url, data=json.dumps(set), headers={
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }).json())
    return response

#paytm portal functions
def get_paytm_accounts(cur_products):
    print('fetch accounts')
    accounts = []
    cur_products.execute("Select * from master_portalaccount where portal_name = 'Paytm'")
    portal = cur_products.fetchone()
    if portal is not None:
        portal_id = portal[0]
        cur_products.execute("Select * from master_portalaccountdetails where portal_id_id = " + str(portal_id))
        portal_accounts = cur_products.fetchall()
        if len(portal_accounts):
            for portal_account in portal_accounts:
                access_token = ''
                merchant_id = ''
                access_token_field = portal_account[4]
                for item in access_token_field:
                    if item['name'] == 'access_token':
                        access_token = item['base_url']
                    elif item['name'] == 'merchant_id':
                        merchant_id = item['base_url']
                    elif item['name'] == 'client_id':
                        client_id = item['base_url']
                    elif item['name'] == 'client_secret_id':
                        client_secret = item['base_url']
                    elif item['name'] == 'code':
                        code = item['base_url']
                    elif item['name'] == 'state':
                        state = item['base_url']
                warehouses = []
                warehouse_field = portal_account[5]
                for item in warehouse_field:
                    warehouse = {
                        'warehouse_id': item['warehouse_id'],
                        'paytm_warehouse_code': item['portal_warehouse']
                    }
                    warehouses.append(warehouse)
                account = {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "code": code,
                    "state": state,
                    "merchant_id": merchant_id,
                    "access_token": access_token,
                    "account_name": portal_account[1],
                    "account_id": int(portal_account[0]),
                    "portal_id": int(portal_id),
                    "warehouses": warehouses
                }
                accounts.append(account)
                print("paytm_account_detailsline132==>",accounts)
    return accounts
#
def get_paytm_stock(merchant_id, access_token, product_id):
    url = "https://seed.paytm.com/v3/merchant/" + str(merchant_id) + "/inventory?authtoken=" + str(
        access_token) + "&product_id=" + str(int(product_id))
    # print("get_paytm_stock==url",url)
    print("get_paytm_stock==url", url)
    try:
        response = requests.get(url, data={'product_id': int(product_id)}).json()
    except:
        response = 'None'

    print("response==140==>", response)
    return response
#
#
def paytm_update_stock(merchant_id, access_token, paytm_set):
    url = "https://seed.paytm.com/v3/merchant/" + str(merchant_id) + "/inventory?authtoken=" + str(access_token)
    data = {'data': paytm_set}
    response = requests.put(url, data=json.dumps(data), headers={'Content-Type': 'application/json'}).json()
    return response

def get_paytm_product(cur_products, product_id):
    product_query = "SELECT paytm_portal_sku, paytm_portal_unique_id, paytm_account_id from paytm_paytmproducts where product_id = " + str(
        product_id)
    cur_products.execute(product_query)
    product = cur_products.fetchone()
    if product is not None:
        return {
            'sku': product[0],
            'unique_id': product[1],
            'account_id': product[2]
        }
    else:
        return None

# Todo: Add new portal functions here


def main():
    print('Create DB Connections')
    client = MongoClient(
        'mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/wms?retryWrites=true&w=majority')
    db = client.wms
    conn_products = psycopg2.connect(database=db_name_products, user=name, password=password,
                                     host=rds_host, port="5432")
    cur_products = conn_products.cursor()
    conn_orders = psycopg2.connect(database=db_name_orders, user=name, password=password,
                                   host=rds_host, port="5432")
    cur_orders = conn_orders.cursor()

    # Create files for the logs of the stock update
    #master files
    master_filename = 'master_refresh_update_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    master_filefrom = '/tmp/' + master_filename
    master_fileto = '/buymore2/bin_reco/master_stock/' + master_filename
    master_update_csv = open(master_filefrom, 'w')
    master_fieldnames = ['buymore_sku', 'product_id', 'warehouse', 'stock', 'status', 'error']
    master_writer = csv.DictWriter(master_update_csv, fieldnames=master_fieldnames)
    master_writer.writeheader()

    #flipkart files
    flipkart_filename = 'master_refresh_flipkart_update_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    flipkart_filefrom = '/tmp/' + flipkart_filename
    flipkart_fileto = '/buymore2/bin_reco/master_stock/' + flipkart_filename
    flipkart_update_csv = open(flipkart_filefrom, 'w')
    flipkart_fieldnames = ['flipkart_portal_sku', 'product_id', 'location', 'inventory', 'status', 'error_code', 'error_desc',
                  'attr_error_code', 'attr_error_desc', 'failed_attribute']
    flipkart_writer = csv.DictWriter(flipkart_update_csv, fieldnames=flipkart_fieldnames)
    flipkart_writer.writeheader()

    #paytm files
    paytm_filename = 'master_refresh_paytm_update_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    paytm_filefrom = '/tmp/' + paytm_filename
    paytm_fileto = '/buymore2/bin_reco/master_stock/' + paytm_filename
    paytm_update_csv = open(paytm_filefrom, 'w')
    paytm_fieldnames = ['warehouse_id', 'product_id', 'quantity', 'message']
    paytm_writer = csv.DictWriter(paytm_update_csv, fieldnames=paytm_fieldnames)
    paytm_writer.writeheader()


    #Todo: Add new stock file here

    #fetch account data
    #flipkart account
    print('Get flipkart accounts')
    accounts = get_flipkart_accounts(cur_products)
    account_data = {account['account_id']: account for account in accounts}

    # paytm account
    paytm_accounts = get_paytm_accounts(cur_products)
    paytm_account_data = {account['account_id']: account for account in paytm_accounts}
    print("paytm_account_data==334===>",paytm_account_data)


    # Todo: Add fetch account code for new portal here
    # Fetch the stocks from the WMS
    pipeline = [
        {
            "$match": {
                "binId": {
                #all commented query are working
                # "$nin": ['/^D/i', '/^R/i', '/^Trial/i']
                # "$nin": [re.compile(r"^D(?i)")]
                "$nin": [re.compile(r"^D(?i)"),re.compile(r"^R(?i)"),re.compile(r"^Trial(?i)")]
                # "$regex": "^(?!D)"
                # "$not":{"$regex": "^D","$options":"i"}
                         }
            }
        },
        {
            "$group": {
                "_id": {
                    # "binId": '$binId',
                    "warehouseId": '$warehouseId',
                    "buymore_sku": '$buymore_sku'
                },
                "total": {
                    "$sum": '$quantity'
                }
            }
        }
    ]

    products_query = "SELECT product_id, buymore_sku from master_masterproduct limit 20"
    cur_products.execute(products_query)
    products = cur_products.fetchall()
    print('Get master products product_id and buymore_sku')
    products_data = {product[1]: product[0] for product in products}
    binreco = db.api_binreco.aggregate(pipeline)

    # Fetch placed orders count for the product and warehouse
    print('Fetch orders of dispatch date greater than and equal to today')
    order_query = "Select product_id, warehouse_id, sum(qty) from api_neworder no inner join api_dispatchdetails dd " \
                  "on no.dd_id = dd.dd_id_id where date(dispatch_by_date) >= CURRENT_DATE and ((dd.fulfillment_model " \
                  "in ('merchant', '') and picklist_id = 0) or (dd.fulfillment_model = 'portal'))  and product_id != " \
                  "0 GROUP BY product_id, warehouse_id "
    cur_orders.execute(order_query)
    new_stocks = cur_orders.fetchall()

    #Store the counts of the orders
    print('Read the orders for the master stock')
    stock_orders = {str(order[0]) + '#' + str(order[1]): order[2] for order in new_stocks}

    count = 0

    # Add variables to handle the stock information for portals
    # Flipkart Inventory Set
    flipkart_inventory_set = {}

    # Paytm Set
    paytm_set = []
    paytm_current_stock = {}

    #Todo: Add New Portal stock variable here

    # Loop through stocks and prepare inventory for portal and master stock
    print('loop through the wms stock')
    for item in binreco:
        buymore_sku = item['_id']['buymore_sku'].strip()

        if buymore_sku in ('', None):
            continue
        try:
            buymore_sku = buymore_sku.replace('\xa0', ' ')
            product_id = products_data[buymore_sku]
        except KeyError as e:
            print(buymore_sku)
            print('key error')
            continue
        buymore_sku = buymore_sku.replace("'", "''")

        warehouse_id = item['_id']['warehouseId']
        stock = int(item['total'])
        key = str(product_id) + '#' + str(warehouse_id)
        if key in stock_orders:
            orders = -1 * abs(stock_orders[key])
        else:
            orders = 0

        if stock < 0:
            stock = 0
        print("product_id===",product_id)
        print("warehouse_id===", warehouse_id)
        print('Check if stock data already exists')
        ##added for testing
        # engine = create_engine(cred_for_sqlalchemy_orders)
        # query = "SELECT * from api_masterstock where product_id=" + str(product_id) + " and warehouse=" + str(warehouse_id)
        # master_stock = pd.read_sql(query, engine)
        engine = create_engine(cred_for_sqlalchemy_orders)
        query = "SELECT * from api_masterstock where product_id=" + str(product_id) + " and warehouse=" + str(
            warehouse_id)
        master_stock_df = pd.read_sql(query, engine)
        engine.dispose()
        if master_stock_df.empty:
            master_stock = True
        else:
            master_stock = master_stock_df

        print("master_stock===>", master_stock)

        # cur_orders.execute(
        #     "SELECT * from api_masterstock where product_id=" + str(product_id) + " and warehouse=" + str(warehouse_id))
        # master_stock2 = cur_orders.fetchone()
        # print("master_stock2", master_stock2)
        # print("master_stock",master_stock)
        quantity = stock + orders
        if quantity < 0:
            quantity = 0

        # Fetch the product and update inventory if avaialable in the portals
        # flipkart Update inventory code
        print('fetch flipkart product')
        flipkart_product = get_flipkart_product(product_id, cur_products)
        if flipkart_product is not None:
            print('Get updated quantity for portal')
            if flipkart_product['account_id'] not in account_data:
                print(str(flipkart_product['account_id']) + ' account id does not exist in record')
            else:
                account = account_data[flipkart_product['account_id']]
                access_token = account['access_token']
                location_id = get_location_id(account, warehouse_id)
                if access_token not in flipkart_inventory_set:
                    flipkart_inventory_set[access_token] = {}
                print('Check for existing sku record in the set')
                if flipkart_product['sku'] in flipkart_inventory_set[access_token]:
                    print('Add the location to the existing set')
                    locations = flipkart_inventory_set[access_token][flipkart_product['sku']]['locations']
                    ff = 0
                    for location in locations:
                        if location['id'] == location_id:
                            ff = 1

                    if ff == 0:
                        locations.append({
                            "id": location_id,
                            "inventory": quantity
                        })
                        flipkart_inventory_set[access_token][flipkart_product['sku']]['locations'] = locations
                else:
                    print('Add new entry')
                    flipkart_inventory_set[access_token][flipkart_product['sku']] = {
                        'product_id': flipkart_product['unique_id'],
                        "locations": [
                            {
                                "id": location_id,
                                "inventory": quantity
                            }
                        ]
                    }
                print('Rec generated:')

                if len(flipkart_inventory_set[access_token]) == 10:
                    print('Request to be generated at this point')
                    res = update_flipkart_inventory(access_token, flipkart_inventory_set[access_token])
                    if 'errors' in res:
                        for sku in flipkart_inventory_set[access_token]:
                            for location in flipkart_inventory_set[access_token][sku]['locations']:

                                num1 = int(location['inventory'])
                                if num1 != 0:
                                    location1 = num1 - 1
                                else:
                                    location1 = 0
                                row = {
                                    'flipkart_portal_sku': sku,
                                    'product_id': flipkart_inventory_set[access_token][sku]['product_id'],
                                    'location': location['id'],
                                    'inventory': location1,
                                    'status': res['errors'][0]['severity'],
                                    'error_code': res['errors'][0]['code'],
                                    'error_desc': res['errors'][0]['description'],
                                    'attr_error_code': '-',
                                    'attr_error_desc': '-',
                                    'failed_attribute': '-'
                                }
                                flipkart_writer.writerow(row)
                    else:
                        for sku in res:
                            try:
                                sku = sku.replace('\xa0', ' ')
                                for location in flipkart_inventory_set[access_token][sku]['locations']:
                                    row = {
                                        'flipkart_portal_sku': sku,
                                        'product_id': flipkart_inventory_set[access_token][sku]['product_id'],
                                        'location': location['id'],
                                        'inventory': location['inventory'],
                                        'status': res[sku]['status']
                                    }
                                    if 'errors' in res[sku]:
                                        row['error_code'] = res[sku]['errors'][0]['code']
                                        row['error_desc'] = res[sku]['errors'][0]['description']
                                    else:
                                        row['error_code'] = '-'
                                        row['error_desc'] = '-'
                                    if 'attribute_errors' in res[sku]:
                                        row['attr_error_code'] = res[sku]['attribute_errors'][0]['code']
                                        row['attr_error_desc'] = res[sku]['attribute_errors'][0]['description']
                                        row['failed_attribute'] = res[sku]['attribute_errors'][0]['attribute']
                                    else:
                                        row['attr_error_code'] = '-'
                                        row['attr_error_desc'] = '-'
                                        row['failed_attribute'] = '-'
                                    flipkart_writer.writerow(row)
                            except KeyError as e:
                                print('Key error:' + sku)
                                continue
                    flipkart_inventory_set[access_token] = {}
        else:
            print('Flipkart Product not found')

        # Paytm Inventory update
        print('fetch paytm product')
        paytm_product = get_paytm_product(cur_products, product_id)
        print("paytm_product===545==>",paytm_product)
        if paytm_product is None:
            continue
        if paytm_product['unique_id'] not in paytm_current_stock:
            account = paytm_account_data[paytm_product['account_id']]
            warehouses = account['warehouses']
            paytm_warehouse = {w['warehouse_id']: w['paytm_warehouse_code'] for w in warehouses}
            print(account['access_token'])
            print(paytm_product['unique_id'])
            print(account['merchant_id'])
            paytm_stock = get_paytm_stock(account['merchant_id'], account['access_token'], paytm_product['unique_id'])
            print("paytm_stock==556===>",paytm_stock)
            paytm_current_stock[str(int(paytm_product['unique_id']))] = {}
            if 'data' in paytm_stock:
                for data in paytm_stock['data']:
                    paytm_row = {
                        "warehouse_id": data['warehouse_id'],
                        "product_id": int(data['product_id']),
                        "quantity": data['quantity'],
                        "status": data['inventory_status']
                    }
                    if data['inventory_status'] == 1:
                        paytm_current_stock[str(int(data['product_id']))][str(data['warehouse_id'])] = data['quantity']
            else:
                continue

        paytm_quantity = quantity - 1
        if paytm_quantity > 50:
            paytm_quantity = 50
        if paytm_quantity < 0:
            paytm_quantity = 0
        try:
            if paytm_quantity == paytm_current_stock[str(int(paytm_product['unique_id']))][str(paytm_warehouse[warehouse_id])]:
                continue
        except:
            continue
        paytm_csv_row = {
            'product_id': str(int(paytm_product['unique_id'])),
            'warehouse_id': str(paytm_warehouse[warehouse_id]),
            'quantity': paytm_quantity
        }
        #
        paytm_set.append(paytm_csv_row)
        if len(paytm_set) == 50:
            res = paytm_update_stock(account['merchant_id'], account['access_token'], paytm_set)
            for item in res['data']:
                paytm_csv_row = {
                    "warehouse_id": item['warehouse_id'],
                    "product_id": item['product_id'],
                    "quantity": item['quantity']
                }
                if 'message' in item:
                    paytm_csv_row['message'] = item['message']
                else:
                    paytm_csv_row['message'] = item['error']
                paytm_writer.writerow(paytm_csv_row)
            paytm_set = []


        #Todo: Add new Portal update inventory code here

    #     #master stock update code
        master_row = {
            'buymore_sku': buymore_sku,
            'product_id': product_id,
            'warehouse': warehouse_id,
            'stock': quantity,
            'status': False,
            'error': ''
        }
        print("master_stock===>===>",master_stock)
        if master_stock is True:
            print('Insert New Record')
            try:
                cur_orders.execute(
                    "Insert into api_masterstock (buymore_sku, product_id, warehouse, stock, orders, lost, status, "
                    "updated_time) VALUES ('" + buymore_sku + "', " + str(
                        product_id) + ", " + str(warehouse_id) + ", " + str(quantity) + ", 0, 0, false, NOW())")
                conn_orders.commit()
            except:
                master_row['error'] = sys.exc_info()[0]
        else:
            print('Update existing Record')
            try:
                cur_orders.execute("Update api_masterstock set stock = " + str(
                    quantity) + ", orders = 0, lost = 0, status = False, updated_time = NOW() where product_id= " + str(
                    product_id) + " and warehouse = " + str(warehouse_id))
                conn_orders.commit()
            except:
                master_row['error'] = sys.exc_info()[0]
        master_writer.writerow(master_row)
        count += 1
    master_update_csv.close()

    # Check for the pending inventory data to be updated for all portals

    # Update flipkart Pending inventory data
    print('check for the pending records')
    if len(flipkart_inventory_set):
        for access_token in flipkart_inventory_set:
            if len(flipkart_inventory_set[access_token]):
                print('Request to be generated here')
                res = update_flipkart_inventory(access_token, flipkart_inventory_set[access_token])
                if 'errors' in res:
                    for sku in flipkart_inventory_set[access_token]:
                        for location in flipkart_inventory_set[access_token][sku]['locations']:
                            num1 = int(location['inventory'])
                            if num1 != 0:
                                location1 = num1 - 1
                            else:
                                location1 = 0
                            row = {
                                'flipkart_portal_sku': sku,
                                'product_id': flipkart_inventory_set[access_token][sku]['product_id'],
                                'location': location['id'],
                                'inventory': location1,
                                'status': res['errors'][0]['severity'],
                                'error_code': res['errors'][0]['code'],
                                'error_desc': res['errors'][0]['description'],
                                'attr_error_code': '-',
                                'attr_error_desc': '-',
                                'failed_attribute': '-'
                            }
                            flipkart_writer.writerow(row)
                else:
                    for sku in res:
                        for location in flipkart_inventory_set[access_token][sku]['locations']:
                            row = {
                                'flipkart_portal_sku': sku,
                                'product_id': flipkart_inventory_set[access_token][sku]['product_id'],
                                'location': location['id'],
                                'inventory': location['inventory'],
                                'status': res[sku]['status']
                            }
                            if 'errors' in res[sku]:
                                row['error_code'] = res[sku]['errors'][0]['code']
                                row['error_desc'] = res[sku]['errors'][0]['description']
                            else:
                                row['error_code'] = '-'
                                row['error_desc'] = '-'
                            if 'attribute_errors' in res[sku]:
                                row['attr_error_code'] = res[sku]['attribute_errors'][0]['code']
                                row['attr_error_desc'] = res[sku]['attribute_errors'][0]['description']
                                row['failed_attribute'] = res[sku]['attribute_errors'][0]['attribute']
                            else:
                                row['attr_error_code'] = '-'
                                row['attr_error_desc'] = '-'
                                row['failed_attribute'] = '-'
                            flipkart_writer.writerow(row)

    flipkart_update_csv.close()

    # Update paytm Pending inventory data
    if len(paytm_set) > 0:
        res = paytm_update_stock(account['merchant_id'], account['access_token'], paytm_set)
        if 'data' in res:
            for item in res['data']:
                paytm_csv_row = {
                    "warehouse_id": item['warehouse_id'],
                    "product_id": item['product_id'],
                    "quantity": item['quantity']
                }
                if 'message' in item:
                    paytm_csv_row['message'] = item['message']
                else:
                    paytm_csv_row['message'] = item['error']
                paytm_writer.writerow(paytm_csv_row)
        else:
            print(res)
    paytm_update_csv.close()



    #Todo: Add New portal Pending inventory update code here


    # upload the log files to server
    print('upload file to dropbox')
    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    dbx = dropbox.Dropbox(access_token)

    # master stock refresh file
    with open(master_filefrom, 'rb') as f:
        dbx.files_upload(f.read(), master_fileto, mode=dropbox.files.WriteMode.overwrite)

    # flipkart stock refresh file
    with open(flipkart_filefrom, 'rb') as f:
        dbx.files_upload(f.read(), flipkart_fileto, mode=dropbox.files.WriteMode.overwrite)

    # paytm stock refresh file
    with open(paytm_filefrom, 'rb') as f:
        dbx.files_upload(f.read(), paytm_fileto, mode=dropbox.files.WriteMode.overwrite)


    # Todo: Add new portal refresh file upload code here

    # Close DB Connections
    conn_products.close()
    conn_orders.close()

    print('Total: ' + str(count))

    return {
        'statusCode': 200,
        'body': json.dumps('The data is generated')
    }


main()

