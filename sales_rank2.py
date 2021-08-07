import mws, math, psycopg2, json
import sys, os
from datetime import datetime, timedelta
import logging


# from throttle import throttle
def db_credential(db_name, typ):
    import requests
    # import json
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
    print(response, type(response))
    if status == True:
        return response['db_detail'][typ]
    else:
        return


db_creds = db_credential('postgres', 'db_detail_for_psycopg2')
# db_creds={"endPoint":"buymore-dev-aurora.cluster-cegnfd8ehfoc.ap-south-1.rds.amazonaws.com","userName":"postgres","passWord":"r2DfZEyyNL2VLfg2"}
RDS_HOST = db_creds['endPoint']
NAME = db_creds['userName']
PASSWORD = db_creds['passWord']

accounts = [
    {
        "account_name": "Amazon",
        "account_id": 1,
        'SELLER_ID': 'A1JMG6531Z3EIJ',
        'MARKETPLACE_ID': 'A21TJRUUN4KGV',
        'DEVELOPER_NO': '7380-1949-4131',
        'ACCESS_KEY': 'AKIAJYQAC3UP6RYQELXQ',
        'SECRET_KEY': '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',
        'MWS_SERVICE_VERSION': '2015-05-01',
        'MWS_CLIENT_VERSION': '2017-03-15',
        'APPLICATION_NAME': 'buymore',
        'APPLICATION_VERSION': '1.0.0'
    }
]
# 61553499472283
# order_item_id='01218380396827'
# select setval('api_employee_emp_id_seq', 251, true)
#
#
# transaction_id===> 1614762342
# last transaction id
# /home/ubuntu/employees/employees/api
# https://stackoverflow.com/questions/4206707/filezilla-error-while-writing-failure
#
# amzn.mws.e6b2d6df-a2bc-53f2-222c-74cb2948683a - Auth Token amazon
# Seller account identifiers for Buy More
# Seller ID: A1JMG6531Z3EIJ
# Marketplace ID: A21TJRUUN4KGV (Amazon.in)
# Developer account identifier and credentials
# Developer Account Number: 7380-1949-4131
# AWS Access Key ID: AKIAJYQAC3UP6RYQELXQ
# Secret Key: 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


# def lambda_handler(event, context):
def lambda_handler():
    try:
        print("inside 1nd try")
        account = accounts[0]
        productsMWS = mws.Products(access_key=account['ACCESS_KEY'], secret_key=account['SECRET_KEY'],
                                   account_id=account['SELLER_ID'], region='IN')
        # productsMWS = mws.Products(access_key='AKIAJYQAC3UP6RYQELXQ', secret_key='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',
        #                            account_id='A1JMG6531Z3EIJ', region='IN')
        print("productsMWS===>",productsMWS)
        RDS_HOST = "buymore-2.cluster-cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
        NAME = "postgres"
        PASSWORD = "buymore3"
        DB_NAME = "products"

        conn_products = psycopg2.connect(host=RDS_HOST, database=DB_NAME, user=NAME, password=PASSWORD)
        products_cursor = conn_products.cursor()
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # if event.get("Records", None) is not None:
        #     data = json.loads(event["Records"][0]["body"])
        # else:
        #     data = event

        # asin_list = data.get("asin_list", None)
        asin_list = ['B07RBR1FRX','B07RWJBV2N','B07RWK9GJ4','B07RWZH8PY']
        # asin_list = ['B07RBR1FRX']
        print("asin_list===",asin_list)
        if asin_list is not None:
            print("inside if")
            try:
                print("inside 2nd try")
                # data = productsMWS.get_maching_product(asins=asin_list, marketplaceid='A21TJRUUN4KGV',
                #                                                       excludeme="True")
                # data = productsMWS.get_maching_products(asins=asin_list, marketplaceid='A21TJRUUN4KGV')
                # data = productsMWS.get_maching_product(asin=asin_list, marketplaceid='A21TJRUUN4KGV')
                data = productsMWS.get_maching_product(asinlist=asin_list, marketplaceid='A21TJRUUN4KGV')

                #marketplaceids
                print("data--",data)
                dataList = data.parsed
                print("dataList-->>",dataList)
            except TypeError as e:
                print("inside 2nd except")
                print("-------" + str(e))
                dataList = []

            if len(dataList) != 0:
                for singleResult in dataList:
                    print("singleResult==",singleResult)
                    # isPriceUpdated = "False"
                    # resultAmountvalue = 0
                    # amazon_upload_selling_price = 0
                    value_listings = []
                    today = datetime.now()
                    # print("singleResult---",singleResult)
                    resultAsin = singleResult.get("ASIN", {})
                    resultStatus = singleResult.get("status", {})
                    asinVal = resultAsin.get("value", None)
                    resultStatusVal = resultStatus.get("value", None)
                    if asinVal is not None and resultStatusVal is not None:

                        if resultStatusVal == "Success":
                            resultProduct = singleResult.get("Product", None)
                            if resultProduct is not None:
                                # resultOfferListings = resultProduct.get("AttributeSets", {})
                                resultAttributeSets = resultAttributeSets.get("AttributeSets", {})

                                # resultPrice = resultLowestOfferListing.get("Price", {})
                                # resultLandedPrice = resultPrice.get("LandedPrice", {})
                                # resultAmount = resultLandedPrice.get("Amount", {})
                                resultSalesRank = resultSalesRank.get("Rank", None)
                                if resultSalesRank is not None:


                                    logger.info("resultSalesRank for asin--------->{0}".format(resultSalesRank))

                                    value_listings = [resultSalesRank, today]

                        firstQuery = "SELECT product_id from amazon_amazonproducts WHERE amazon_unique_id =  '{0}' ".format(
                            asinVal)

                        products_cursor.execute(firstQuery)
                        resVal = products_cursor.fetchone()


                        logger.info("resultSalesRank for asin--------->{0}".format(resultSalesRank))
                        query_write = "UPDATE master_masterproduct SET sales_rank=%s, updated_at=%s WHERE product_id= {0} ".format(product_id)
                        products_cursor.execute(query_write, [resultSalesRank, today])
                        conn_products.commit()
                        logger.info("updated for asin--------->{0}".format(asinVal))

        products_cursor.close()
        conn_products.close()
    except Exception as e:
        logger.info("Exception----------------->{0}".format(str(e)))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(exc_type, exc_tb.tb_lineno)
        print(" first excpet call")
    finally:
        # products_cursor.close()
        # conn_products.close()
        return {'statusCode': 200, 'body': "Done"}
print(lambda_handler())
