import json
import csv
from datetime import datetime
import psycopg2
import requests
import os
import dropbox

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


#Portal Functions

# flipkart api functions
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
    product_query = "SELECT flipkart_portal_sku, flipkart_portal_unique_id, flipkart_account_id from flipkart_flipkartproducts where product_id = " + str(
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


def update_flipkart_inventory(access_token, set):
    url = 'https://api.flipkart.net/sellers/listings/v3/update/inventory'
    print(url)
    print(set)
    response = dict(requests.post(url, data=json.dumps(set), headers={
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }).json())
    print(response)
    return response

# paytm account functions
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
    return accounts


def get_paytm_stock(merchant_id, access_token, product_id):
    url = "https://seed.paytm.com/v3/merchant/" + str(merchant_id) + "/inventory?authtoken=" + str(
        access_token) + "&product_id=" + str(int(product_id))
    # print("get_paytm_stock==url",url)
    print("get_paytm_stock==url", url)
    print("product_id====>", product_id)
    try:
        response = requests.get(url, data={'product_id': int(product_id)}).json()
    except:
        response = 'None'

    print("response==get_paytm_stock==140==>", response)
    return response


def paytm_update_stock(merchant_id, access_token, paytm_set):
    url = "https://seed.paytm.com/v3/merchant/" + str(merchant_id) + "/inventory?authtoken=" + str(access_token)
    print("paytm_update_stock=====>",url)
    data1 = {'data': paytm_set}
    data2 = json.dumps(data1)

    print("data2===",data2)
    response = requests.put(url, data=json.dumps(data1), headers={'Content-Type': 'application/json'}).json()
    print("paytm_update_stock--response-->",response)
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

#snapdeal account functions
def get_snapdeal_accounts(cur_products):
    print('fetch accounts')
    cur_products.execute("Select * from master_portalaccount where portal_name = 'snapdealp'")
    portal = cur_products.fetchone()
    accounts = []
    if portal is not None:
        portal_id = portal[0]
        cur_products.execute("Select * from master_portalaccountdetails where portal_id_id = " + str(portal_id))
        portal_accounts = cur_products.fetchall()
        if len(portal_accounts):
            for portal_account in portal_accounts:
                xauthtoken = ''
                xsellerauthztoken = ''
                client_id = ''
                access_token_field = portal_account[4]
                for item in access_token_field:
                    if item['name'] == 'xauthtoken':
                        xauthtoken = item['base_url']
                    elif item['name'] == 'xsellerauthztoken':
                        xsellerauthztoken = item['base_url']
                    elif item['name'] == 'clientid':
                        client_id = item['base_url']
                warehouses = []
                warehouse_field = portal_account[5]
                for item in warehouse_field:
                    warehouse = {
                        'warehouse_id': item['warehouse_id'],
                        'warehouse_code': item['portal_warehouse'],
                        'xsellerauthztoken': xsellerauthztoken
                    }
                    warehouses.append(warehouse)
                account = {
                    "portal_name": portal[1],
                    "account_name": portal_account[1],
                    "portal_id": portal_id,
                    "account_id": portal_account[0],
                    "xauthtoken":xauthtoken,
                    "client_id":client_id,
                    "warehouses": warehouses
                }
                accounts.append(account)
    return accounts


def get_snapdeal_product(cur_products, product_id):
    product_query = "SELECT snapdealp_portal_sku, snapdealp_portal_unique_id, snapdealp_account_id from snapdealp_snapdealpproducts where product_id = " + str(product_id)
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


def get_snapdeal_stock(supc, account, xsellerauthztoken):
    url = 'https://apigateway.snapdeal.com/seller-api/products/'+ str(supc) +'/inventory'
    headers = {
        'x-auth-token': account['xauthtoken'],
        'X-Seller-AuthZ-Token': xsellerauthztoken,
        'clientId':account['client_id']
    }
    # response = requests.get(url, headers=headers).json()
    # print(response)
    # return response
    try:
        response = requests.get(url, headers=headers).json()
    except:
        response = 'None'

    # print("get_snapdeal_stock_response==250==>", response)
    return response



def update_snapdeal_stock(set, account, xsellerauthztoken):
    url = 'https://apigateway.snapdeal.com/seller-api/products/inventory'
    print("update_snapdeal_stock-->",url)
    headers = {
        'x-auth-token': account['xauthtoken'],
        'X-Seller-AuthZ-Token': xsellerauthztoken,
        'clientId': account['client_id'],
        'Content-Type': 'application/json'
    }

    # print("update_snapdeal===set",set)
    # print("update_snapdeal===xsellerauthztoken", xsellerauthztoken)
    # print("update_snapdeal===account", account)
    response = requests.post(url, data=json.dumps(set), headers=headers).json()
    print("update_snapdeal_stock==response=>>>>",response)
    return response


def verify_stock_update(account, xsellerauthztoken, uploadid, page):

    print('uploadid==', uploadid)
    url = 'http://apigateway.snapdeal.com/seller-api/feed/result/' + str(uploadid) + '?pageNumber=' + str(page)
    print(url)
    headers = {
        'x-auth-token': account['xauthtoken'],
        'X-Seller-AuthZ-Token': xsellerauthztoken,
        'clientId': account['client_id']
    }
    response = requests.get(url, headers=headers).json()
    print("verify_stock_update_respmse>>", response)
    return response

# Todo: Add New Account Functions Here

# def lambda_handler(event, context):
def lambda_handler():
    print('Create DB Connections')
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
    conn_products = psycopg2.connect(host=rds_host,
                                    database=db_name_products,
                                    user=name,
                                    password=password)
    cur_products = conn_products.cursor()
    conn_orders = psycopg2.connect(host=rds_host,
                                    database=db_name_orders,
                                    user=name,
                                    password=password)
    cur_orders = conn_orders.cursor()
    # Create Files for the file status

    # flipkart file
    flipkart_file_name = 'flipkart_partial_portal_update_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    flipkart_file_from = '/tmp/' + flipkart_file_name
    flipkart_file_to = '/buymore2/bin_reco/master_stock/' + flipkart_file_name
    flipkart_portal_update_csv = open(flipkart_file_from, 'w')
    flipkart_fieldnames = ['flipkart_portal_sku', 'location', 'inventory', 'status', 'error_code', 'error_desc',
                           'attr_error_code', 'attr_error_desc', 'failed_attribute']
    flipkart_writer = csv.DictWriter(flipkart_portal_update_csv, fieldnames=flipkart_fieldnames)
    flipkart_writer.writeheader()

    # paytm file
    paytm_file_name = 'paytm_partial_portal_update_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    paytm_file_from = '/tmp/' + paytm_file_name
    paytm_file_to = '/buymore2/bin_reco/master_stock/' + paytm_file_name
    paytm_portal_update_csv = open(paytm_file_from, 'w')
    paytm_fieldnames = ['warehouse_id', 'product_id', 'quantity', 'message']
    paytm_writer = csv.DictWriter(paytm_portal_update_csv, fieldnames=paytm_fieldnames)
    paytm_writer.writeheader()


    # Todo: Add new portal files code here

    # Get Accounts for the portals
    #get flipkart accounts
    flipkart_accounts = get_flipkart_accounts(cur_products)
    flipkart_account_data = {account['account_id']: account for account in flipkart_accounts}

    # get paytm accounts
    paytm_accounts = get_paytm_accounts(cur_products)
    paytm_account_data = {account['account_id']: account for account in paytm_accounts}
    print("paytm_account_data=====>",paytm_account_data)
    paytm_merchant_id=paytm_account_data[9]['merchant_id']
    print("paytm_merchant_id====>",paytm_merchant_id)
    paytm_access_token=paytm_account_data[9]['access_token']
    print("paytm_access_token===>",paytm_access_token)

    # get snapdeal accounts
    snapdeal_accounts = get_snapdeal_accounts(cur_products)
    snapdeal_account_data = {account['account_id']: account for account in snapdeal_accounts}

    #Todo: Add new portal accounts code here

    # Fetch the orders for the partial portal update
    print('Fetch orders for partial portal update')
    stock_query = "select * from api_masterstock where status = true"
    cur_orders.execute(stock_query)
    stocks = cur_orders.fetchall()
    count = 0

    # declare dictionary/array for the bulk update data and supporting variables
    #flipkart inventory set
    flipkart_inventory_set = {}

    #paytm inventory set
    paytm_set = []
    paytm_current_stock = {}

    #Todo: Add the variables for the new portal inventory variables here

    # Loop through stocks fetched from master stock and update the stock in master table
    print('loop through the wms stock')
    update_stock_ids = []
    for stock in stocks:
        print(count)
        product_id = stock[2]
        print(product_id)
        warehouse_id = stock[3]
        quantity = stock[4] + stock[5] + stock[6]
        if quantity < 0:
            quantity = 0
        cur_orders.execute("Update api_masterstock set stock = " + str(
            quantity) + ", orders = 0, lost = 0, status = False, updated_time = NOW() where product_id= " + str(
            product_id) + " and warehouse = " + str(warehouse_id))
        conn_orders.commit()
        update_stock_ids.append(stock[0])
        count += 1
    # check if any stock to be updated
    if len(update_stock_ids) == 0:
        return {
            'status': False,
            'message': 'Empty stock list'
        }

    # Fetch the stocks updated
    update_stock_ids_str = ",".join([str(id) for id in update_stock_ids])
    stock_query = "select * from api_masterstock where masterstock_id in (" + update_stock_ids_str + ")"
    cur_orders.execute(stock_query)
    stocks = cur_orders.fetchall()
    for stock in stocks:
        product_id = stock[2]
        print(product_id)
        warehouse_id = stock[3]
        quantity = stock[4]
        if quantity < 0:
            quantity = 0
        # Add the Portal Set Update code

        # Flipkart Update
        print('fetch flipkart product')
        flipkart_product = get_flipkart_product(product_id, cur_products)
        print(flipkart_product)
        if flipkart_product is not None:
            if flipkart_product['account_id'] not in flipkart_account_data:
                print(str(flipkart_product['account_id']) + ' account id does not exist in record')
            else:
                account = flipkart_account_data[flipkart_product['account_id']]
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
                print("test111", flipkart_inventory_set[access_token][flipkart_product['sku']])
                if len(flipkart_inventory_set[access_token]) == 1000:
                    print('Request to be generated at this point')
                    print(access_token)
                    print(flipkart_inventory_set[access_token])
                    # res = update_flipkart_inventory(access_token, flipkart_inventory_set[access_token])
                    for i in flipkart_inventory_set[access_token]:
                        res = update_flipkart_inventory(access_token, {i: flipkart_inventory_set[access_token][i]})
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
                                        'location': location['id'],
                                        'inventory': location1,
                                        'status': res['errors'][0]['severnity'],
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
                                    num1 = int(location['inventory'])
                                    if num1 != 0:
                                        location1 = num1 - 1
                                    else:
                                        location1 = 0
                                    row = {
                                        'flipkart_portal_sku': sku,
                                        'location': location['id'],
                                        'inventory': location1,
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
                    flipkart_inventory_set[access_token] = {}
        else:
            print('Flipkart Product not found')

        # Paytm update
        paytm_product = get_paytm_product(cur_products, product_id)
        if paytm_product is None:
            continue
        if paytm_product['unique_id'] not in paytm_current_stock:
            account = paytm_account_data[paytm_product['account_id']]
            warehouses = account['warehouses']
            paytm_warehouse = {w['warehouse_id']: w['paytm_warehouse_code'] for w in warehouses}
            paytm_stock = get_paytm_stock(account['merchant_id'], account['access_token'], paytm_product['unique_id'])
            paytm_current_stock[str(int(paytm_product['unique_id']))] = {}
            if 'data' in paytm_stock:
                for data in paytm_stock['data']:
                    if data['inventory_status'] == 1:
                        paytm_current_stock[str(int(data['product_id']))][str(data['warehouse_id'])] = data[
                            'quantity']
            else:
                continue
        print(paytm_current_stock)
        ###because of error commented this part
        # if quantity == paytm_current_stock[str(int(paytm_product['unique_id']))][
        #     str(paytm_warehouse[warehouse_id])]:
        #     continue
        ###because of error commented this part
        paytm_quantity = quantity - 1
        if paytm_quantity < 0:
            paytm_quantity = 0
        if paytm_quantity > 50:
            paytm_quantity = 50
        paytm_csv_row = {
            'product_id': str(int(paytm_product['unique_id'])),
            'warehouse_id': str(paytm_warehouse[warehouse_id]),
            'quantity': paytm_quantity
        }
        print(paytm_csv_row)
        paytm_set.append(paytm_csv_row)

        if len(paytm_set) == 50:

            res = paytm_update_stock(paytm_merchant_id, paytm_access_token, paytm_set)
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


        #Todo: Add new portal update code here

    print('check for the pending records')
    # Check for the pending inventory values in the declared set
    #flipkart pending inventory set
    if len(flipkart_inventory_set):
        for access_token in flipkart_inventory_set:
            if len(flipkart_inventory_set[access_token]):
                print('Request to be generated here')
                print(access_token)
                print(flipkart_inventory_set[access_token])
                for i in flipkart_inventory_set[access_token]:
                    res = update_flipkart_inventory(access_token, {i: flipkart_inventory_set[access_token][i]})

                    if 'errors' in res:
                        for sku in flipkart_inventory_set[access_token]:
                            for location in flipkart_inventory_set[access_token][sku]['locations']:
                                row = {
                                    'flipkart_portal_sku': sku,
                                    'location': location['id'],
                                    'inventory': location['inventory'],
                                    'status': res['errors'][0]['severnity'],
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
    flipkart_portal_update_csv.close()
    # paytm pending inventory set
    if len(paytm_set) > 0:

        res = paytm_update_stock(paytm_merchant_id, paytm_access_token, paytm_set)
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
    paytm_portal_update_csv.close()


    #Todo: Add new portal pending inventory code here

    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    dbx = dropbox.Dropbox(access_token)
    # Write files to Dropbox
    # flipkart file
    with open(flipkart_file_from, 'rb') as f:
        data = dbx.files_upload(f.read(), flipkart_file_to, mode=dropbox.files.WriteMode.overwrite)
    # paytm file
    with open(paytm_file_from, 'rb') as f:
        data = dbx.files_upload(f.read(), paytm_file_to, mode=dropbox.files.WriteMode.overwrite)

    #Todo: Add new portal file upload code here
    ###snapdeal code here

    # snapdeal file
    snapdeal_file_name = 'snapdeal_partial_portal_update_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    snapdeal_file_from = '/tmp/' + snapdeal_file_name
    snapdeal_file_to = '/buymore2/bin_reco/master_stock/' + snapdeal_file_name
    snapdeal_portal_update_csv = open(snapdeal_file_from, 'w')
    snapdeal_fieldnames = ['product_id', 'quantity', 'message', 'status']
    snapdeal_writer = csv.DictWriter(snapdeal_portal_update_csv, fieldnames=snapdeal_fieldnames)
    snapdeal_writer.writeheader()

    # Todo: Add new portal files code here

    # Get Accounts for the portals
    # get snapdeal accounts
    snapdeal_accounts = get_snapdeal_accounts(cur_products)
    snapdeal_account_data = {account['account_id']: account for account in snapdeal_accounts}

    # Todo: Add new portal accounts code here

    # snapdeal inventory set
    snapdeal_current_stock = {}
    snapdeal_set = {}
    snapdeal_qty = {}
    uploadids = []

    # Todo: Add the variables for the new portal inventory variables here

    # Fetch the stocks updated
    update_stock_ids_str = ",".join([str(id) for id in update_stock_ids])
    stock_query = "select * from api_masterstock where masterstock_id in (" + update_stock_ids_str + ")"
    cur_orders.execute(stock_query)
    stocks = cur_orders.fetchall()
    for stock in stocks:
        product_id = stock[2]
        print(product_id)
        warehouse_id = stock[3]
        quantity = stock[4]
        if quantity < 0:
            quantity = 0
        # Add the Portal Set Update code

        # snapdeal stock
        snapdeal_product = get_snapdeal_product(cur_products, product_id)
        if snapdeal_product is None:
            continue
        if snapdeal_product['unique_id'] not in snapdeal_current_stock:
            account = snapdeal_account_data[snapdeal_product['account_id']]
            warehouses = account['warehouses']
            # xsellerauthztoken = warehouses[0]['xsellerauthztoken']
            ##hard_coded
            xsellerauthztoken = 'ea3571d5-0fd4-4035-8200-b97dac283bbd'
            snapdeal_stock = get_snapdeal_stock(snapdeal_product['unique_id'], account, xsellerauthztoken)
            if str(snapdeal_product['unique_id']) not in snapdeal_current_stock:
                snapdeal_current_stock[str(snapdeal_product['unique_id'])] = {}
                print("snapdeal_current_stock--303->", snapdeal_current_stock)
            try:
                if 'message' not in snapdeal_stock and snapdeal_stock['payload']['live']:
                    snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = \
                        snapdeal_stock['payload']['availableInventory']
                    print("snapdeal_current_stock--308->", snapdeal_current_stock)
                else:
                    continue
            except:
                print("some error occured")
                continue
        ##because of error commented this code
        # if quantity == snapdeal_current_stock[str(snapdeal_product['unique_id'])][str(xsellerauthztoken)]:
        #     continue

        snapdeal_quantity = quantity - 5
        if snapdeal_quantity < 0:
            snapdeal_quantity = 0
        if snapdeal_quantity > 50:
            snapdeal_quantity = 50
        snapdeal_csv_row = {
            'supc': str(snapdeal_product['unique_id']),
            'availableInventory': snapdeal_quantity
        }

        if xsellerauthztoken in snapdeal_set:
            snapdeal_set[xsellerauthztoken]['data'].append(snapdeal_csv_row)
        else:
            snapdeal_set[xsellerauthztoken] = {
                'account': account,
                'data': [
                    snapdeal_csv_row
                ]
            }
        if len(snapdeal_set[xsellerauthztoken]['data']) == 50:
            res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                        snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
            if 'message' in res['metadata']:
                message = res['metadata']['message']
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        'status': 'Failed'
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                message = res['payload']['uploadId']
                uploadids.append(message)
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    if message not in snapdeal_qty:
                        snapdeal_qty[message] = {}
                    snapdeal_qty[message][item['supc']] = item['availableInventory']

            snapdeal_set[xsellerauthztoken]['data'] = []

        # Todo: Add new portal update code here

    print('check for the pending records')
    # Check for the pending inventory values in the declared set
    # snapdeal pending inventory set
    if len(snapdeal_set):
        for xsellerauthztoken in snapdeal_set:
            if len(snapdeal_set[xsellerauthztoken]['data']) > 0:
                res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                            snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
                if 'message' in res['metadata']:
                    message = res['metadata']['message']
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        snapdeal_csv_row = {
                            'product_id': item['supc'],
                            'quantity': item['availableInventory'],
                            'message': message,
                            'status': 'Failed'
                        }
                        snapdeal_writer.writerow(snapdeal_csv_row)
                else:
                    message = res['payload']['uploadId']
                    uploadids.append(message)
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        if message not in snapdeal_qty:
                            snapdeal_qty[message] = {}
                        snapdeal_qty[message][item['supc']] = item['availableInventory']

    # #Write snapdeal file with uploadids status
    for message in uploadids:
        page = 0
        total_pages = 1
        while page < total_pages:
            # res = verify_stock_update(account, xsellerauthztoken, message, page)
            res = verify_stock_update(account, xsellerauthztoken, message, page + 1)
            page = page + 1
            if 'message' in res['metadata']:
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    status = res['metadata']['message']
                    if status == " Update request is under process, Please try after 3 minutes":
                        final_status = 'success'
                    else:
                        final_status = res['metadata']['message']
                    print("final_status==", final_status)
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        # 'status': res['metadata']['message']
                        'status': final_status
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                results = res['payload']['supcResults']
                upload_id = res['payload']['uploadId']
                page = res['payload']['currentPage']
                total_pages = res['payload']['totalPages']

                for result in results:
                    # print("result===<<",result)
                    try:
                        quantity1 = snapdeal_current_stock[result['supc']][xsellerauthztoken]
                        # quantity=snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = snapdeal_stock['payload']['availableInventory']
                        print("quantity--try==", quantity1)
                    except:
                        quantity1 = 0
                        print("quantity--except==", quantity1)
                    print("final_quantity", quantity1)
                    q = quantity1
                    print("snapdeal_quantity===inner==", snapdeal_quantity)
                    print("result===", result)
                    snapdeal_csv_row = {
                        'product_id': result['supc'],
                        # 'quantity': snapdeal_quantity[upload_id][result['supc']],
                        'quantity': q,
                        # 'quantity': snapdeal_quantity,
                        'message': upload_id,
                        'status': result['state']
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
    snapdeal_portal_update_csv.close()

    # Todo: Add new portal pending inventory code here

    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    dbx = dropbox.Dropbox(access_token)
    # Write files to Dropbox
    # snapdeal file
    with open(snapdeal_file_from, 'rb') as f:
        data = dbx.files_upload(f.read(), snapdeal_file_to, mode=dropbox.files.WriteMode.overwrite)

    # Todo: Add new portal file upload code here

    # snapdeal file
    snapdeal_file_name = 'snapdeal_partial_portal_update_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    snapdeal_file_from = '/tmp/' + snapdeal_file_name
    snapdeal_file_to = '/buymore2/bin_reco/master_stock/' + snapdeal_file_name
    snapdeal_portal_update_csv = open(snapdeal_file_from, 'w')
    snapdeal_fieldnames = ['product_id', 'quantity', 'message', 'status']
    snapdeal_writer = csv.DictWriter(snapdeal_portal_update_csv, fieldnames=snapdeal_fieldnames)
    snapdeal_writer.writeheader()

    # Todo: Add new portal files code here

    # Get Accounts for the portals
    # get snapdeal accounts
    snapdeal_accounts = get_snapdeal_accounts(cur_products)
    snapdeal_account_data = {account['account_id']: account for account in snapdeal_accounts}

    # Todo: Add new portal accounts code here

    # Fetch the orders for the partial portal update


    # declare dictionary/array for the bulk update data and supporting variables
    # snapdeal inventory set
    snapdeal_current_stock = {}
    snapdeal_set = {}
    snapdeal_qty = {}
    uploadids = []

    # Todo: Add the variables for the new portal inventory variables here

    # Loop through stocks fetched from master stock and update the stock in master table

    # check if any stock to be updated

    # Fetch the stocks updated
    update_stock_ids_str = ",".join([str(id) for id in update_stock_ids])
    stock_query = "select * from api_masterstock where masterstock_id in (" + update_stock_ids_str + ")"
    cur_orders.execute(stock_query)
    stocks = cur_orders.fetchall()
    for stock in stocks:
        product_id = stock[2]
        print(product_id)
        warehouse_id = stock[3]
        quantity = stock[4]
        if quantity < 0:
            quantity = 0
        # Add the Portal Set Update code

        # snapdeal stock
        snapdeal_product = get_snapdeal_product(cur_products, product_id)
        if snapdeal_product is None:
            continue
        if snapdeal_product['unique_id'] not in snapdeal_current_stock:
            account = snapdeal_account_data[snapdeal_product['account_id']]
            warehouses = account['warehouses']
            # xsellerauthztoken = warehouses[0]['xsellerauthztoken']
            ##hard_coded
            xsellerauthztoken = '41d11ace-a9ce-44af-ba2d-c39d4d284749'
            snapdeal_stock = get_snapdeal_stock(snapdeal_product['unique_id'], account, xsellerauthztoken)
            if str(snapdeal_product['unique_id']) not in snapdeal_current_stock:
                snapdeal_current_stock[str(snapdeal_product['unique_id'])] = {}
                print("snapdeal_current_stock--303->", snapdeal_current_stock)
            try:
                if 'message' not in snapdeal_stock and snapdeal_stock['payload']['live']:
                    snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = \
                        snapdeal_stock['payload']['availableInventory']
                    print("snapdeal_current_stock--308->", snapdeal_current_stock)
                else:
                    continue
            except:
                print("some error occured")
                continue
        ##because of error commented this code
        # if quantity == snapdeal_current_stock[str(snapdeal_product['unique_id'])][str(xsellerauthztoken)]:
        #     continue

        snapdeal_quantity = quantity - 5
        if snapdeal_quantity < 0:
            snapdeal_quantity = 0
        if snapdeal_quantity > 50:
            snapdeal_quantity = 50
        snapdeal_csv_row = {
            'supc': str(snapdeal_product['unique_id']),
            'availableInventory': snapdeal_quantity
        }

        if xsellerauthztoken in snapdeal_set:
            snapdeal_set[xsellerauthztoken]['data'].append(snapdeal_csv_row)
        else:
            snapdeal_set[xsellerauthztoken] = {
                'account': account,
                'data': [
                    snapdeal_csv_row
                ]
            }
        if len(snapdeal_set[xsellerauthztoken]['data']) == 50:
            res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                        snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
            if 'message' in res['metadata']:
                message = res['metadata']['message']
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        'status': 'Failed'
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                message = res['payload']['uploadId']
                uploadids.append(message)
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    if message not in snapdeal_qty:
                        snapdeal_qty[message] = {}
                    snapdeal_qty[message][item['supc']] = item['availableInventory']

            snapdeal_set[xsellerauthztoken]['data'] = []

        # Todo: Add new portal update code here

    print('check for the pending records')
    # Check for the pending inventory values in the declared set
    # snapdeal pending inventory set
    if len(snapdeal_set):
        for xsellerauthztoken in snapdeal_set:
            if len(snapdeal_set[xsellerauthztoken]['data']) > 0:
                res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                            snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
                if 'message' in res['metadata']:
                    message = res['metadata']['message']
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        snapdeal_csv_row = {
                            'product_id': item['supc'],
                            'quantity': item['availableInventory'],
                            'message': message,
                            'status': 'Failed'
                        }
                        snapdeal_writer.writerow(snapdeal_csv_row)
                else:
                    message = res['payload']['uploadId']
                    uploadids.append(message)
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        if message not in snapdeal_qty:
                            snapdeal_qty[message] = {}
                        snapdeal_qty[message][item['supc']] = item['availableInventory']

    # #Write snapdeal file with uploadids status
    for message in uploadids:
        page = 0
        total_pages = 1
        while page < total_pages:
            # res = verify_stock_update(account, xsellerauthztoken, message, page)
            res = verify_stock_update(account, xsellerauthztoken, message, page + 1)
            page = page + 1
            if 'message' in res['metadata']:
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    status = res['metadata']['message']
                    if status == " Update request is under process, Please try after 3 minutes":
                        final_status = 'success'
                    else:
                        final_status = res['metadata']['message']
                    print("final_status==", final_status)
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        # 'status': res['metadata']['message']
                        'status': final_status
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                results = res['payload']['supcResults']
                upload_id = res['payload']['uploadId']
                page = res['payload']['currentPage']
                total_pages = res['payload']['totalPages']

                for result in results:
                    # print("result===<<",result)
                    try:
                        quantity1 = snapdeal_current_stock[result['supc']][xsellerauthztoken]
                        # quantity=snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = snapdeal_stock['payload']['availableInventory']
                        print("quantity--try==", quantity1)
                    except:
                        quantity1 = 0
                        print("quantity--except==", quantity1)
                    print("final_quantity", quantity1)
                    q = quantity1
                    print("snapdeal_quantity===inner==", snapdeal_quantity)
                    print("result===", result)
                    snapdeal_csv_row = {
                        'product_id': result['supc'],
                        # 'quantity': snapdeal_quantity[upload_id][result['supc']],
                        'quantity': q,
                        # 'quantity': snapdeal_quantity,
                        'message': upload_id,
                        'status': result['state']
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
    snapdeal_portal_update_csv.close()

    # Todo: Add new portal pending inventory code here

    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    dbx = dropbox.Dropbox(access_token)
    # Write files to Dropbox
    # snapdeal file
    with open(snapdeal_file_from, 'rb') as f:
        data = dbx.files_upload(f.read(), snapdeal_file_to, mode=dropbox.files.WriteMode.overwrite)

    # Todo: Add new portal file upload code here

    # snapdeal file
    snapdeal_file_name = 'snapdeal_partial_portal_update_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    snapdeal_file_from = '/tmp/' + snapdeal_file_name
    snapdeal_file_to = '/buymore2/bin_reco/master_stock/' + snapdeal_file_name
    snapdeal_portal_update_csv = open(snapdeal_file_from, 'w')
    snapdeal_fieldnames = ['product_id', 'quantity', 'message', 'status']
    snapdeal_writer = csv.DictWriter(snapdeal_portal_update_csv, fieldnames=snapdeal_fieldnames)
    snapdeal_writer.writeheader()

    # Todo: Add new portal files code here

    # Get Accounts for the portals
    # get snapdeal accounts
    snapdeal_accounts = get_snapdeal_accounts(cur_products)
    snapdeal_account_data = {account['account_id']: account for account in snapdeal_accounts}

    # Todo: Add new portal accounts code here

    # Fetch the orders for the partial portal update
    # count = 0

    # declare dictionary/array for the bulk update data and supporting variables
    # snapdeal inventory set
    snapdeal_current_stock = {}
    snapdeal_set = {}
    snapdeal_qty = {}
    uploadids = []

    # Todo: Add the variables for the new portal inventory variables here

    # Loop through stocks fetched from master stock and update the stock in master table
    print('loop through the wms stock')

    # Fetch the stocks updated
    update_stock_ids_str = ",".join([str(id) for id in update_stock_ids])
    stock_query = "select * from api_masterstock where masterstock_id in (" + update_stock_ids_str + ")"
    cur_orders.execute(stock_query)
    stocks = cur_orders.fetchall()
    for stock in stocks:
        product_id = stock[2]
        print(product_id)
        warehouse_id = stock[3]
        quantity = stock[4]
        if quantity < 0:
            quantity = 0
        # Add the Portal Set Update code

        # snapdeal stock
        snapdeal_product = get_snapdeal_product(cur_products, product_id)
        if snapdeal_product is None:
            continue
        if snapdeal_product['unique_id'] not in snapdeal_current_stock:
            account = snapdeal_account_data[snapdeal_product['account_id']]
            warehouses = account['warehouses']
            # xsellerauthztoken = warehouses[0]['xsellerauthztoken']
            ##hard_coded
            xsellerauthztoken = '6c658a34-1edb-4710-af6b-b8aef47a6928'
            snapdeal_stock = get_snapdeal_stock(snapdeal_product['unique_id'], account, xsellerauthztoken)
            if str(snapdeal_product['unique_id']) not in snapdeal_current_stock:
                snapdeal_current_stock[str(snapdeal_product['unique_id'])] = {}
                print("snapdeal_current_stock--303->", snapdeal_current_stock)
            try:
                if 'message' not in snapdeal_stock and snapdeal_stock['payload']['live']:
                    snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = \
                        snapdeal_stock['payload']['availableInventory']
                    print("snapdeal_current_stock--308->", snapdeal_current_stock)
                else:
                    continue
            except:
                print("some error occured")
                continue
        ##because of error commented this code
        # if quantity == snapdeal_current_stock[str(snapdeal_product['unique_id'])][str(xsellerauthztoken)]:
        #     continue

        snapdeal_quantity = quantity - 5
        if snapdeal_quantity < 0:
            snapdeal_quantity = 0
        if snapdeal_quantity > 50:
            snapdeal_quantity = 50
        snapdeal_csv_row = {
            'supc': str(snapdeal_product['unique_id']),
            'availableInventory': snapdeal_quantity
        }

        if xsellerauthztoken in snapdeal_set:
            snapdeal_set[xsellerauthztoken]['data'].append(snapdeal_csv_row)
        else:
            snapdeal_set[xsellerauthztoken] = {
                'account': account,
                'data': [
                    snapdeal_csv_row
                ]
            }
        if len(snapdeal_set[xsellerauthztoken]['data']) == 50:
            res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                        snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
            if 'message' in res['metadata']:
                message = res['metadata']['message']
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        'status': 'Failed'
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                message = res['payload']['uploadId']
                uploadids.append(message)
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    if message not in snapdeal_qty:
                        snapdeal_qty[message] = {}
                    snapdeal_qty[message][item['supc']] = item['availableInventory']

            snapdeal_set[xsellerauthztoken]['data'] = []

        # Todo: Add new portal update code here

    print('check for the pending records')
    # Check for the pending inventory values in the declared set
    # snapdeal pending inventory set
    if len(snapdeal_set):
        for xsellerauthztoken in snapdeal_set:
            if len(snapdeal_set[xsellerauthztoken]['data']) > 0:
                res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                            snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
                if 'message' in res['metadata']:
                    message = res['metadata']['message']
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        snapdeal_csv_row = {
                            'product_id': item['supc'],
                            'quantity': item['availableInventory'],
                            'message': message,
                            'status': 'Failed'
                        }
                        snapdeal_writer.writerow(snapdeal_csv_row)
                else:
                    message = res['payload']['uploadId']
                    uploadids.append(message)
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        if message not in snapdeal_qty:
                            snapdeal_qty[message] = {}
                        snapdeal_qty[message][item['supc']] = item['availableInventory']

    # #Write snapdeal file with uploadids status
    for message in uploadids:
        page = 0
        total_pages = 1
        while page < total_pages:
            # res = verify_stock_update(account, xsellerauthztoken, message, page)
            res = verify_stock_update(account, xsellerauthztoken, message, page + 1)
            page = page + 1
            if 'message' in res['metadata']:
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    status = res['metadata']['message']
                    if status == " Update request is under process, Please try after 3 minutes":
                        final_status = 'success'
                    else:
                        final_status = res['metadata']['message']
                    print("final_status==", final_status)
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        # 'status': res['metadata']['message']
                        'status': final_status
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                results = res['payload']['supcResults']
                upload_id = res['payload']['uploadId']
                page = res['payload']['currentPage']
                total_pages = res['payload']['totalPages']

                for result in results:
                    # print("result===<<",result)
                    try:
                        quantity1 = snapdeal_current_stock[result['supc']][xsellerauthztoken]
                        # quantity=snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = snapdeal_stock['payload']['availableInventory']
                        print("quantity--try==", quantity1)
                    except:
                        quantity1 = 0
                        print("quantity--except==", quantity1)
                    print("final_quantity", quantity1)
                    q = quantity1
                    print("snapdeal_quantity===inner==", snapdeal_quantity)
                    print("result===", result)
                    snapdeal_csv_row = {
                        'product_id': result['supc'],
                        # 'quantity': snapdeal_quantity[upload_id][result['supc']],
                        'quantity': q,
                        # 'quantity': snapdeal_quantity,
                        'message': upload_id,
                        'status': result['state']
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
    snapdeal_portal_update_csv.close()

    # Todo: Add new portal pending inventory code here

    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    dbx = dropbox.Dropbox(access_token)
    # Write files to Dropbox
    # snapdeal file
    with open(snapdeal_file_from, 'rb') as f:
        data = dbx.files_upload(f.read(), snapdeal_file_to, mode=dropbox.files.WriteMode.overwrite)

    # Todo: Add new portal file upload code here

    # close db connections
    # conn_products.close()
    # conn_orders.close()

    # snapdeal file
    snapdeal_file_name = 'snapdeal_partial_portal_update_' + str(int(datetime.timestamp(datetime.now()))) + '.csv'
    snapdeal_file_from = '/tmp/' + snapdeal_file_name
    snapdeal_file_to = '/buymore2/bin_reco/master_stock/' + snapdeal_file_name
    snapdeal_portal_update_csv = open(snapdeal_file_from, 'w')
    snapdeal_fieldnames = ['product_id', 'quantity', 'message', 'status']
    snapdeal_writer = csv.DictWriter(snapdeal_portal_update_csv, fieldnames=snapdeal_fieldnames)
    snapdeal_writer.writeheader()

    # Todo: Add new portal files code here

    # Get Accounts for the portals
    # get snapdeal accounts
    snapdeal_accounts = get_snapdeal_accounts(cur_products)
    snapdeal_account_data = {account['account_id']: account for account in snapdeal_accounts}

    # Todo: Add new portal accounts code here

    # Fetch the orders for the partial portal update
    # count = 0

    # declare dictionary/array for the bulk update data and supporting variables
    # snapdeal inventory set
    snapdeal_current_stock = {}
    snapdeal_set = {}
    snapdeal_qty = {}
    uploadids = []

    # Todo: Add the variables for the new portal inventory variables here

    # Loop through stocks fetched from master stock and update the stock in master table
    print('loop through the wms stock')

    # Fetch the stocks updated
    update_stock_ids_str = ",".join([str(id) for id in update_stock_ids])
    stock_query = "select * from api_masterstock where masterstock_id in (" + update_stock_ids_str + ")"
    cur_orders.execute(stock_query)
    stocks = cur_orders.fetchall()
    for stock in stocks:
        product_id = stock[2]
        print(product_id)
        warehouse_id = stock[3]
        quantity = stock[4]
        if quantity < 0:
            quantity = 0
        # Add the Portal Set Update code

        # snapdeal stock
        snapdeal_product = get_snapdeal_product(cur_products, product_id)
        if snapdeal_product is None:
            continue
        if snapdeal_product['unique_id'] not in snapdeal_current_stock:
            account = snapdeal_account_data[snapdeal_product['account_id']]
            warehouses = account['warehouses']
            # xsellerauthztoken = warehouses[0]['xsellerauthztoken']
            ##hard_coded
            xsellerauthztoken = '9381cf6a-b03e-4ba9-a098-3584e124439b'
            snapdeal_stock = get_snapdeal_stock(snapdeal_product['unique_id'], account, xsellerauthztoken)
            if str(snapdeal_product['unique_id']) not in snapdeal_current_stock:
                snapdeal_current_stock[str(snapdeal_product['unique_id'])] = {}
                print("snapdeal_current_stock--303->", snapdeal_current_stock)
            try:
                if 'message' not in snapdeal_stock and snapdeal_stock['payload']['live']:
                    snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = \
                        snapdeal_stock['payload']['availableInventory']
                    print("snapdeal_current_stock--308->", snapdeal_current_stock)
                else:
                    continue
            except:
                print("some error occured")
                continue
        ##because of error commented this code
        # if quantity == snapdeal_current_stock[str(snapdeal_product['unique_id'])][str(xsellerauthztoken)]:
        #     continue

        snapdeal_quantity = quantity - 5
        if snapdeal_quantity < 0:
            snapdeal_quantity = 0
        if snapdeal_quantity > 50:
            snapdeal_quantity = 50
        snapdeal_csv_row = {
            'supc': str(snapdeal_product['unique_id']),
            'availableInventory': snapdeal_quantity
        }

        if xsellerauthztoken in snapdeal_set:
            snapdeal_set[xsellerauthztoken]['data'].append(snapdeal_csv_row)
        else:
            snapdeal_set[xsellerauthztoken] = {
                'account': account,
                'data': [
                    snapdeal_csv_row
                ]
            }
        if len(snapdeal_set[xsellerauthztoken]['data']) == 50:
            res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                        snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
            if 'message' in res['metadata']:
                message = res['metadata']['message']
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        'status': 'Failed'
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                message = res['payload']['uploadId']
                uploadids.append(message)
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    if message not in snapdeal_qty:
                        snapdeal_qty[message] = {}
                    snapdeal_qty[message][item['supc']] = item['availableInventory']

            snapdeal_set[xsellerauthztoken]['data'] = []

        # Todo: Add new portal update code here

    print('check for the pending records')
    # Check for the pending inventory values in the declared set
    # snapdeal pending inventory set
    if len(snapdeal_set):
        for xsellerauthztoken in snapdeal_set:
            if len(snapdeal_set[xsellerauthztoken]['data']) > 0:
                res = update_snapdeal_stock(snapdeal_set[xsellerauthztoken]['data'],
                                            snapdeal_set[xsellerauthztoken]['account'], xsellerauthztoken)
                if 'message' in res['metadata']:
                    message = res['metadata']['message']
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        snapdeal_csv_row = {
                            'product_id': item['supc'],
                            'quantity': item['availableInventory'],
                            'message': message,
                            'status': 'Failed'
                        }
                        snapdeal_writer.writerow(snapdeal_csv_row)
                else:
                    message = res['payload']['uploadId']
                    uploadids.append(message)
                    for item in snapdeal_set[xsellerauthztoken]['data']:
                        if message not in snapdeal_qty:
                            snapdeal_qty[message] = {}
                        snapdeal_qty[message][item['supc']] = item['availableInventory']

    # #Write snapdeal file with uploadids status
    for message in uploadids:
        page = 0
        total_pages = 1
        while page < total_pages:
            # res = verify_stock_update(account, xsellerauthztoken, message, page)
            res = verify_stock_update(account, xsellerauthztoken, message, page + 1)
            page = page + 1
            if 'message' in res['metadata']:
                for item in snapdeal_set[xsellerauthztoken]['data']:
                    status = res['metadata']['message']
                    if status == " Update request is under process, Please try after 3 minutes":
                        final_status = 'success'
                    else:
                        final_status = res['metadata']['message']
                    print("final_status==", final_status)
                    snapdeal_csv_row = {
                        'product_id': item['supc'],
                        'quantity': item['availableInventory'],
                        'message': message,
                        # 'status': res['metadata']['message']
                        'status': final_status
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
            else:
                results = res['payload']['supcResults']
                upload_id = res['payload']['uploadId']
                page = res['payload']['currentPage']
                total_pages = res['payload']['totalPages']

                for result in results:
                    # print("result===<<",result)
                    try:
                        quantity1 = snapdeal_current_stock[result['supc']][xsellerauthztoken]
                        # quantity=snapdeal_current_stock[str(snapdeal_stock['payload']['supc'])][str(xsellerauthztoken)] = snapdeal_stock['payload']['availableInventory']
                        print("quantity--try==", quantity1)
                    except:
                        quantity1 = 0
                        print("quantity--except==", quantity1)
                    print("final_quantity", quantity1)
                    q = quantity1
                    print("snapdeal_quantity===inner==", snapdeal_quantity)
                    print("result===", result)
                    snapdeal_csv_row = {
                        'product_id': result['supc'],
                        # 'quantity': snapdeal_quantity[upload_id][result['supc']],
                        'quantity': q,
                        # 'quantity': snapdeal_quantity,
                        'message': upload_id,
                        'status': result['state']
                    }
                    snapdeal_writer.writerow(snapdeal_csv_row)
    snapdeal_portal_update_csv.close()

    # Todo: Add new portal pending inventory code here

    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    dbx = dropbox.Dropbox(access_token)
    # Write files to Dropbox
    # snapdeal file
    with open(snapdeal_file_from, 'rb') as f:
        data = dbx.files_upload(f.read(), snapdeal_file_to, mode=dropbox.files.WriteMode.overwrite)

    # Todo: Add new portal file upload code here

    # close db connections
    conn_products.close()
    conn_orders.close()


    #close db connections
    conn_products.close()
    conn_orders.close()

    print('Total: ' + str(count))

    return {
        'statusCode': 200,
        'body': json.dumps('The data is generated')
    }

print(lambda_handler())