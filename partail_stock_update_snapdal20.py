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
    try:
        response = requests.get(url, headers=headers).json()
    except:
        response = 'None'
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

    print("update_snapdeal===set",set)
    print("update_snapdeal===xsellerauthztoken", xsellerauthztoken)
    print("update_snapdeal===account", account)
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

    #Todo: Add new portal accounts code here

    # Fetch the orders for the partial portal update
    print('Fetch orders for partial portal update')
    stock_query = "select * from api_masterstock where status = true"
    cur_orders.execute(stock_query)
    stocks = cur_orders.fetchall()
    count = 0

    # declare dictionary/array for the bulk update data and supporting variables
    #snapdeal inventory set
    snapdeal_current_stock = {}
    snapdeal_set = {}
    snapdeal_qty = {}
    uploadids = []

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

        #Todo: Add new portal update code here

    print('check for the pending records')
    # Check for the pending inventory values in the declared set
    #snapdeal pending inventory set
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

    #Todo: Add new portal pending inventory code here

    access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
    dbx = dropbox.Dropbox(access_token)
    # Write files to Dropbox
    #snapdeal file
    with open(snapdeal_file_from, 'rb') as f:
        data = dbx.files_upload(f.read(), snapdeal_file_to, mode=dropbox.files.WriteMode.overwrite)

    #Todo: Add new portal file upload code here

    #close db connections
    conn_products.close()
    conn_orders.close()

    print('Total: ' + str(count))

    return {
        'statusCode': 200,
        'body': json.dumps('The data is generated')
    }

print(lambda_handler())