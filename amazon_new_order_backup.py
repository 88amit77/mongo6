import mws, os
from datetime import datetime, timedelta
import psycopg2
import time


def get_product(portal_sku, cur_products):
    cur_products.execute(
        "Select product_id from amazon_amazonproducts where amazon_portal_sku = '" + portal_sku.replace("'",
                                                                                                        "''") + "'")
    amazon_product = cur_products.fetchone()
    if amazon_product is not None:
        product_id = amazon_product[0]
        cur_products.execute(
            'Select buymore_sku, product_mrp, hsn_code_id_id from master_masterproduct where product_id = ' + str(
                product_id))
        master_product = cur_products.fetchone()
        if master_product is not None:
            return {
                'buymore_sku': master_product[0],
                'mrp': master_product[1],
                'hsn_code': master_product[2],
                'product_id': product_id
            }

        else:
            # cur_products.execute('SELECT product_id, buymore_sku, product_mrp, hsn_code_id_id FROM master_masterproduct ORDER BY RANDOM() LIMIT 1')
            # product = cur_products.fetchone()
            # print(product)
            product_id = 0
            buymore_sku = 'N/A'
            mrp = 0
            hsn_code = 0
            return {
                'buymore_sku': buymore_sku,
                'mrp': mrp,
                'hsn_code': hsn_code,
                'product_id': product_id
            }
    else:
        # cur_products.execute('SELECT product_id, buymore_sku, product_mrp, hsn_code_id_id FROM master_masterproduct ORDER BY RANDOM() LIMIT 1')
        # product = cur_products.fetchone()
        product_id = 0
        buymore_sku = 'N/A'
        mrp = 0
        hsn_code = 0
        return {
            'buymore_sku': buymore_sku,
            'mrp': mrp,
            'hsn_code': hsn_code,
            'product_id': product_id
        }


def get_hsn_rate(hsn, cur_products):
    cur_products.execute(
        "Select max_rate, min_rate, threshold_amount from calculation_hsncoderate where hsn_rate_id = " + str(hsn))
    hsn_code = cur_products.fetchone()
    if hsn_code is not None:
        return {
            'threshold_amount': hsn_code[2],
            'max_rate': hsn_code[0],
            'min_rate': hsn_code[1]
        }
    else:
        return {
            'threshold_amount': 1000,
            'max_rate': 0.12,
            'min_rate': 0.05
        }


def get_accounts():
    accounts = []
    conn_products = psycopg2.connect(database="products", user="postgres", password="buymore2",
                                     host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_products = conn_products.cursor()
    cur_products.execute("Select * from master_portalaccount where portal_name = 'Amazon'")
    portal = cur_products.fetchone()
    if portal is not None:
        portal_id = portal[0]
        cur_products.execute("Select * from master_portalaccountdetails where portal_id_id = " + str(portal_id))
        portal_accounts = cur_products.fetchall()

        if len(portal_accounts):
            for portal_account in portal_accounts:
                seller_id = ''
                auth_token = ''
                marketplace_id = ''
                developer_no = ''
                access_key = ''
                secret_key = ''
                mws_service_version = ''
                mws_client_version = ''
                application_name = ''
                application_version = ''
                finance_event_report_type = ''
                finance_event_order_refund_report_type = ''
                stock_update_vendor = ''
                returns_report = ''
                reimbursements_report = ''
                access_token_field = portal_account[4]

                for item in access_token_field:
                    if item['name'] == 'seller_id':
                        seller_id = item['base_url']
                    elif item['name'] == 'auth_token':
                        auth_token = item['base_url']
                    elif item['name'] == 'marketplace_id':
                        marketplace_id = item['base_url']
                    elif item['name'] == 'developer_no':
                        developer_no = item['base_url']
                    elif item['name'] == 'access_key':
                        access_key = item['base_url']
                    elif item['name'] == 'secret_key':
                        secret_key = item['base_url']
                    elif item['name'] == 'mws_service_version':
                        mws_service_version = item['base_url']
                    elif item['name'] == 'mws_client_version':
                        mws_client_version = item['base_url']
                    elif item['name'] == 'application_name':
                        application_name = item['base_url']
                    elif item['name'] == 'application_version':
                        application_version = item['base_url']
                    elif item['name'] == 'finance_event_report_type':
                        finance_event_report_type = item['base_url']
                    elif item['name'] == 'finance_event_order_refund_report_type':
                        finance_event_order_refund_report_type = item['base_url']
                    elif item['name'] == 'stock_update_vendor':
                        stock_update_vendor = item['base_url']
                    elif item['name'] == 'returns_report':
                        returns_report = item['base_url']
                    elif item['name'] == 'reimbursements_report':
                        reimbursements_report = item['base_url']

                account = {
                    "account_name": portal_account[1],
                    "account_id": portal_account[0],
                    "portal_id": portal_id,
                    'SELLER_ID': seller_id,
                    'AUTH_TOKEN': auth_token,
                    'MARKETPLACE_ID': marketplace_id,
                    'DEVELOPER_NO': developer_no,
                    'ACCESS_KEY': access_key,
                    'SECRET_KEY': secret_key,
                    'MWS_SERVICE_VERSION': mws_service_version,
                    'MWS_CLIENT_VERSION': mws_client_version,
                    'APPLICATION_NAME': application_name,
                    'APPLICATION_VERSION': application_version,
                    'FINANCE_EVENT_REPORT_TYPE': finance_event_report_type,
                    'FINANCE_EVENT_ORDER_REFUND_REPORT_TYPE': finance_event_order_refund_report_type,
                    'STOCK_UPDATE_VENDOR': stock_update_vendor,
                    'RETURNS_REPORT': returns_report,
                    'REIMBURSMENTS_REPORT': reimbursements_report
                }
                accounts.append(account)
    conn_products.close()
    return accounts


def lambda_handler(event, context):
    now_time = datetime.now()
    now_hour = now_time.hour
    if now_hour % 6 == 0:
        min_time = now_time.replace(hour=now_hour, minute=30)
        max_time = now_time.replace(hour=now_hour, minute=45)
    else:
        min_time = now_time.replace(hour=0, minute=30)
        max_time = now_time.replace(hour=0, minute=45)
    print(min_time)
    print(max_time)
    print(now_time)
    conn_orders = psycopg2.connect(database="orders", user="postgres", password="buymore2",
                                   host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    conn_products = psycopg2.connect(database="products", user="postgres", password="buymore2",
                                     host="buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com", port="5432")
    cur_orders = conn_orders.cursor()
    cur_products = conn_products.cursor()

    accounts = get_accounts()
    exists_order_item_id_list = []
    new_order_items = []
    cur_orders.execute('Select no.order_item_id from api_neworder no  where no.portal_id = 1')
    existing_orders = cur_orders.fetchall()
    if len(existing_orders):
        for order in existing_orders:
            exists_order_item_id_list.append(order[0])
    for account in accounts:
        portal_account_id = account['account_id']
        orders_api = mws.Orders(
            access_key=account['ACCESS_KEY'],
            secret_key=account['SECRET_KEY'],
            account_id=account['SELLER_ID'],
            auth_token=account['AUTH_TOKEN'],
            region='IN'
        )

        # HTTP POST
        # POST /Orders/2013-09-01?AWSAccessKeyId=AKIAJYQAC3UP6RYQELXQ
        #   &Action=ListOrders
        #   &SellerId=A1JMG6531Z3EIJ
        #   &MWSAuthToken=A1JMG6531Z3EIJ
        #   &SignatureVersion=2
        #   &Timestamp=2020-04-09T11%3A32%3A18Z
        #   &Version=2013-09-01
        #   &Signature=81sbZdI%2F9LUMDEqViNdADmlhTTStO4vjfOLEDu1crgs%3D
        #   &SignatureMethod=HmacSHA256
        #   &CreatedAfter=2020-03-31T18%3A30%3A00Z
        #   &CreatedBefore=2020-04-08T18%3A30%3A00Z
        #   &MarketplaceId.Id.1=A21TJRUUN4KGV HTTP/1.1
        # Host: mws.amazonservices.in
        # x-amazon-user-agent: AmazonJavascriptScratchpad/1.0 (Language=Javascript)
        # Content-Type: text/xml

        #  created_after=None, created_before=None,
        #                     lastupdatedafter=None, lastupdatedbefore=None, orderstatus=(),
        #                     fulfillment_channels=(), payment_methods=(), buyer_email=None,
        #                     seller_orderid=None, max_results='100', next_token=None
        if now_time >= min_time and now_time <= max_time:
            print('3 days')
            service_status = orders_api.list_orders(marketplaceids=(account['MARKETPLACE_ID']),
                                                    created_after=datetime.now() - timedelta(days=3))
            created_after=datetime.now() - timedelta(days=3)
            print("created_after=====",created_after)
        else:
            print('5 hours')
            service_status = orders_api.list_orders(marketplaceids=(account['MARKETPLACE_ID']),
                                                    created_after=datetime.now() - timedelta(hours=5))
            created_after=datetime.now() - timedelta(hours=5)
            print("created_after=====",created_after)

        orders_response = service_status.parsed
        print("orders_response===",orders_response)
        orders = orders_response['Orders']['Order']
        print("orders220====", orders)
        for order in orders:
            try:
                print(order)
                order_id = order['AmazonOrderId']['value']
                print("order_id==",order_id)
                fulfillment_channel = order['FulfillmentChannel']['value']
                if fulfillment_channel == 'MFN':
                    fulfillment_model = 'merchant'
                else:
                    fulfillment_model = 'portal'
                time.sleep(2)
                order_items_req = orders_api.list_order_items(amazon_order_id=order_id)
                order_items_response = order_items_req.parsed
                order_items = order_items_response['OrderItems']['OrderItem']
                if isinstance(order_items, list):
                    for item in order_items:
                        print(item)
                        order_item_id = item['OrderItemId']['value']
                        if order_item_id in exists_order_item_id_list:
                            continue
                        order_date = order['PurchaseDate']['value']
                        dispatch_by_date = order['LatestShipDate']['value']
                        ###comented for selling price deference
                        # if 'OrderTotal' in order:
                        #     selling_price = order['OrderTotal']['Amount']['value']
                        # else:
                        #     selling_price = 0

                        if 'ShippingAddress' in order:
                            address = order['ShippingAddress']['City']['value'] + ', ' + \
                                      order['ShippingAddress']['StateOrRegion']['value'] + ', ' + \
                                      order['ShippingAddress']['CountryCode']['value']
                            pincode = order['ShippingAddress']['PostalCode']['value']
                        else:
                            address = 'N/A'
                            pincode = 'N/A'

                        payment_method = order['PaymentMethodDetails']['PaymentMethodDetail']['value']
                        ##added code
                        # selling_price = item['ItemPrice']['Amount']['value']

                        # qty = item['QuantityOrdered']['value']
                        qty = item['NumberOfItems']['value']

                        sku = item['SellerSKU']['value']
                        product_data = get_product(sku, cur_products)
                        product_id = product_data['product_id']
                        product_id1 = product_data['product_id']
                        list1 = []
                        cur_products.execute(
                            "select amazon_upload_selling_price from amazon_amazonproducts where product_id =" + str(
                                product_id1))
                        existing_orders = cur_products.fetchall()
                        if len(existing_orders):
                            for pro in existing_orders:
                                list1.append(pro[0])
                        print("list1", list1)
                        amazon_upload_selling_price_val = list1[0]
                        print("amazon_upload_selling_price_val---->", amazon_upload_selling_price_val)
                        selling_price1 = item['ItemPrice']['Amount']['value'] - item['PromotionDiscount']['Amount'][
                            'value']
                        print("selling_price1_extracted(item-promotion)", selling_price1)

                        if selling_price1 <= amazon_upload_selling_price_val:
                            selling_price = selling_price1
                        else:
                            #selling_price = amazon_upload_selling_price_val
                            selling_price = amazon_upload_selling_price_val*float(qty)
                        mrp = product_data['mrp']
                        buymore_sku = product_data['buymore_sku']
                        hsn_code = product_data['hsn_code']
                        if hsn_code is not None:
                            hsn_code = get_hsn_rate(hsn_code, cur_products)
                            if float(selling_price) > float(hsn_code['threshold_amount']):
                                hsn_rate = hsn_code['max_rate'] * 100
                            else:
                                hsn_rate = hsn_code['min_rate'] * 100
                        else:
                            hsn_rate = 0
                        print('order_id==', order_id)
                        print('order_item_id==', order_item_id)
                        print('order_date==', order_date)
                        print('dispatch_by_date==', dispatch_by_date)
                        # print('portal_sku==', portal_sku)
                        print('qty==', qty)
                        print('selling_price==', selling_price)
                        print('mrp==', mrp)
                        print('payment_method==', payment_method)
                        print('product_id==', product_id)
                        print('tax_rate==', hsn_rate)
                        print('buymore_sku==', buymore_sku)

                        cur_orders.execute(
                            "INSERT INTO api_neworder (order_id, order_item_id, order_date, dispatch_by_date,"
                            "portal_sku, qty, selling_price, mrp, warehouse_id, payment_method, product_id, "
                            "portal_id, portal_account_id, tax_rate, region, buymore_sku) VALUES ('" +
                            str(order_id) + "', '" + str(order_item_id) + "', '" + str(order_date) + "', '"
                            + str(dispatch_by_date) + "', '" + str(sku.replace("'", "''")) + "', '" + str(
                                qty) + "', '" +
                            str(selling_price) + "', '" + str(mrp) + "', 0, '" + str(payment_method) + "', '"
                            + str(product_id) + "', 1, '" + str(portal_account_id) + "', '" + str(hsn_rate)
                            + "', 'N/A', '" + str(buymore_sku.replace("'", "''")) + "') RETURNING dd_id")
                        dd_id = cur_orders.fetchone()[0]
                        if fulfillment_model == 'portal':
                            cur_orders.execute(
                                "INSERT INTO api_dispatchdetails(name, address, pincode, location_latitude, location_longitude, email_id, phone, status, is_mark_placed, have_invoice_file, packing_status, is_dispatch, dispatch_confirmed, is_canceled,is_shipment_create, l_b_h_w, \"bin_Id\", picklist_id, awb, courier_partner, shipment_id, cancel_inward_bin, dd_id_id, created_at, update_at, dd_paymentstatus, dd_cancelledpaymentstatus, fulfillment_model) VALUES('N/A', '" + address + "', '" + str(
                                    pincode) + "', '0','0','N/A', 'N/A', 'dispatched', True, False, True, True, False,  False, False,'N/A', '{}', 0, 'N/A', 'N/A', 'N/A', 0, " + str(
                                    dd_id) + ", now(), now(), False, False, '" + fulfillment_model + "')")
                        else:
                            cur_orders.execute(
                                "INSERT INTO api_dispatchdetails(name, address, pincode, location_latitude, location_longitude, email_id, phone, status, is_mark_placed, have_invoice_file, packing_status, is_dispatch, dispatch_confirmed, is_canceled,is_shipment_create, l_b_h_w, \"bin_Id\", picklist_id, awb, courier_partner, shipment_id, cancel_inward_bin, dd_id_id, created_at, update_at, dd_paymentstatus, dd_cancelledpaymentstatus, fulfillment_model) VALUES('N/A', '" + address + "', '" + str(
                                    pincode) + "', '0','0','N/A', 'N/A', 'created', True, True, False, False, False,  False, False,'N/A', '{}', 0, 'N/A', 'N/A', 'N/A', 0, " + str(
                                    dd_id) + ", now(), now(), False, False, '" + fulfillment_model + "')")
                        conn_orders.commit()
                else:
                    print(order_items)
                    order_item_id = order_items['OrderItemId']['value']
                    if order_item_id in exists_order_item_id_list:
                        continue
                    order_date = order['PurchaseDate']['value']
                    dispatch_by_date = order['LatestShipDate']['value']

                    # if 'OrderTotal' in order:
                    #     selling_price = order['OrderTotal']['Amount']['value']
                    # else:
                    #     selling_price = 0
                    if 'ShippingAddress' in order:
                        address = order['ShippingAddress']['City']['value'] + ', ' + \
                                  order['ShippingAddress']['StateOrRegion']['value'] + ', ' + \
                                  order['ShippingAddress']['CountryCode']['value']
                        pincode = order['ShippingAddress']['PostalCode']['value']
                    else:
                        address = 'N/A'
                        pincode = 'N/A'
                    payment_method = order['PaymentMethodDetails']['PaymentMethodDetail']['value']

                    qty = order_items['QuantityOrdered']['value']
                    sku = order_items['SellerSKU']['value']
                    product_data = get_product(sku, cur_products)
                    product_id = product_data['product_id']
                    product_id1 = product_data['product_id']
                    list1 = []
                    cur_products.execute(
                        "select amazon_upload_selling_price from amazon_amazonproducts where product_id =" + str(
                            product_id1))
                    existing_orders = cur_products.fetchall()
                    if len(existing_orders):
                        for pro in existing_orders:
                            list1.append(pro[0])
                    print("list1", list1)
                    amazon_upload_selling_price_val = list1[0]
                    print("amazon_upload_selling_price_val---->", amazon_upload_selling_price_val)
                    # test=item['ItemPrice']['Amount']['value']
                    # selling_price1 = item['ItemPrice']['Amount']['value'] - item['PromotionDiscount']['Amount']['value']
                    # print("selling_price1_extracted(item-promotion)",selling_price1)

                    # if selling_price1 <= amazon_upload_selling_price_val:
                    #     selling_price = selling_price1
                    # else:
                    # selling_price = amazon_upload_selling_price_val
                    selling_price = amazon_upload_selling_price_val*float(qty)
                    mrp = product_data['mrp']
                    buymore_sku = product_data['buymore_sku']
                    hsn_code = product_data['hsn_code']
                    if hsn_code is not None:
                        hsn_code = get_hsn_rate(hsn_code, cur_products)
                        if float(selling_price) > float(hsn_code['threshold_amount']):
                            hsn_rate = hsn_code['max_rate'] * 100
                        else:
                            hsn_rate = hsn_code['min_rate'] * 100
                    else:
                        hsn_rate = 0

                    print('order_id==', order_id)
                    print('order_item_id==', order_item_id)
                    print('order_date==', order_date)
                    print('dispatch_by_date==', dispatch_by_date)
                    # print('portal_sku==',portal_sku)
                    print('qty==', qty)
                    print('selling_price==', selling_price)
                    print('mrp==', mrp)

                    print('payment_method==', payment_method)
                    print('product_id==', product_id)
                    print('tax_rate==', hsn_rate)
                    print('buymore_sku==', buymore_sku)

                    cur_orders.execute(
                        "INSERT INTO api_neworder (order_id, order_item_id, order_date, dispatch_by_date,"
                        "portal_sku, qty, selling_price, mrp, warehouse_id, payment_method, product_id, "
                        "portal_id, portal_account_id, tax_rate, region, buymore_sku) VALUES ('" +
                        str(order_id) + "', '" + str(order_item_id) + "', '" + str(order_date) + "', '"
                        + str(dispatch_by_date) + "', '" + str(sku.replace("'", "''")) + "', '" + str(qty) + "', '" +
                        str(selling_price) + "', '" + str(mrp) + "', 0, '" + str(payment_method) + "', '"
                        + str(product_id) + "', 1, '" + str(portal_account_id) + "', '" + str(hsn_rate)
                        + "', 'N/A', '" + str(buymore_sku.replace("'", "''")) + "') RETURNING dd_id")
                    dd_id = cur_orders.fetchone()[0]
                    if fulfillment_model == 'portal':
                        cur_orders.execute(
                            "INSERT INTO api_dispatchdetails(name, address, pincode, location_latitude, location_longitude, email_id, phone, status, is_mark_placed, have_invoice_file, packing_status, is_dispatch, dispatch_confirmed, is_canceled,is_shipment_create, l_b_h_w, \"bin_Id\", picklist_id, awb, courier_partner, shipment_id, cancel_inward_bin, dd_id_id, created_at, update_at, dd_paymentstatus, dd_cancelledpaymentstatus, fulfillment_model) VALUES('N/A', '" + address + "', '" + str(
                                pincode) + "', '0','0','N/A', 'N/A', 'dispatched', True, False, True, True, False,  False, False,'N/A', '{}', 0, 'N/A', 'N/A', 'N/A', 0, " + str(
                                dd_id) + ", now(), now(), False, False, '" + fulfillment_model + "')")
                    else:
                        cur_orders.execute(
                            "INSERT INTO api_dispatchdetails(name, address, pincode, location_latitude, location_longitude, email_id, phone, status, is_mark_placed, have_invoice_file, packing_status, is_dispatch, dispatch_confirmed, is_canceled,is_shipment_create, l_b_h_w, \"bin_Id\", picklist_id, awb, courier_partner, shipment_id, cancel_inward_bin, dd_id_id, created_at, update_at, dd_paymentstatus, dd_cancelledpaymentstatus, fulfillment_model) VALUES('N/A', '" + address + "', '" + str(
                                pincode) + "', '0','0','N/A', 'N/A', 'created', True, True, False, False, False,  False, False,'N/A', '{}', 0, 'N/A', 'N/A', 'N/A', 0, " + str(
                                dd_id) + ", now(), now(), False, False, '" + fulfillment_model + "')")
                    conn_orders.commit()
            except KeyError as e:
                print(e)
                continue
    conn_orders.close()
    conn_products.close()

    return {"status": True}