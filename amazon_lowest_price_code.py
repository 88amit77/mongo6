import mws, math, psycopg2, json
import sys, os
from datetime import datetime, timedelta
import logging

from sqlalchemy import create_engine
# from django.db import connection
import json
import requests


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


accounts = [
    {
        "account_name": "Amazon",
        "account_id": 1,
        'SELLER_ID': 'A1JMG6531Z3EIJ',
        'MARKETPLACE_ID': 'A21TJRUUN4KGV',
        'DEVELOPER_NO': '7380-1949-4131',
        'ACCESS_KEY': 'AKIAIJRON4KB4MMAISAQ',
        'SECRET_KEY': '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',
        'MWS_SERVICE_VERSION': '2015-05-01',
        'MWS_CLIENT_VERSION': '2017-03-15',
        'APPLICATION_NAME': 'buymore',
        'APPLICATION_VERSION': '1.0.0'
    }
]


# def lambda_handler(event, context):
def lambda_handler():
    try:
        account = accounts[0]
        # print(event)
        db_name = 'postgres'
        credential = db_credential(db_name)
        print("credential====", credential)
        cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
        print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
        ##products
        cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
        print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)
        ###psycopg2 connection
        cred_for_psycopg2 = credential["response"]["db_detail"]["db_detail_for_psycopg2"]
        rds_host = cred_for_psycopg2['endPoint']
        name = cred_for_psycopg2['userName']
        password = cred_for_psycopg2['passWord']
        ##products
        db_name = "products"

        productsMWS = mws.Products(access_key=account['ACCESS_KEY'], secret_key=account['SECRET_KEY'],
                                   account_id=account['SELLER_ID'], region='IN')

        # RDS_HOST = "buymore-2.cluster-cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
        # NAME = "postgres"
        # PASSWORD = "buymore3"
        # DB_NAME = "products"

        conn_products = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        products_cursor = conn_products.cursor()
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # if event.get("Records", None) is not None:
        #     data = json.loads(event["Records"][0]["body"])
        # else:
        #     data = event
        #
        # asin_list = data.get("asin_list", None)
        # asin_list = ['B01N5LNUKX','B09B9NGKNQ']
        asin_list = ['B01N5LNUKX']
        for asin_list1 in asin_list:
            if asin_list1 is not None:
                try:
                    print("asin_list1==", asin_list1)
                    data = productsMWS.get_lowest_offer_listings_for_asin(asins=asin_list1,
                                                                          marketplaceid='A21TJRUUN4KGV',
                                                                          excludeme="True")
                    dataList = data.parsed
                    print("datalist==>", dataList)
                except TypeError as e:
                    print("-------" + str(e))
                    dataList = []

                if len(dataList) != 0:
                    # for singleResult in dataList:
                    # print("singleResult==>",singleResult)
                    print("data_list==>>", dataList)
                    isPriceUpdated = "False"
                    resultAmountvalue = 0
                    amazon_upload_selling_price = 0
                    value_listings = []
                    today = datetime.now()
                    # print("singleResult---",singleResult)
                    resultAsin = dataList.get("ASIN", {})
                    resultStatus = dataList.get("status", {})
                    asinVal = resultAsin.get("value", None)
                    resultStatusVal = resultStatus.get("value", None)
                    print(resultStatusVal)
                    if asinVal is not None and resultStatusVal is not None:

                        if resultStatusVal == "Success":
                            resultProduct = dataList.get("Product", None)
                            if resultProduct is not None:
                                resultOfferListings = resultProduct.get("LowestOfferListings", {})
                                resultListings = resultOfferListings.get("LowestOfferListing", {})
                                if isinstance(resultListings, list):
                                    resultLowestOfferListing = resultListings[0] if len(resultListings) != 0 else {}
                                else:
                                    resultLowestOfferListing = resultListings
                                resultPrice = resultLowestOfferListing.get("Price", {})
                                resultLandedPrice = resultPrice.get("LandedPrice", {})
                                resultAmount = resultLandedPrice.get("Amount", {})
                                resultAmountvalue = resultAmount.get("value", None)
                                if resultAmountvalue is not None:
                                    isPriceUpdated = "True"

                                    logger.info("resultAmountvalue for asin--------->{0}".format(resultAmountvalue))

                                    value_listings = [resultAmountvalue, today]

                        firstQuery = "SELECT product_id,amazon_upload_selling_price from amazon_amazonproducts WHERE amazon_unique_id =  '{0}' ".format(
                            asinVal)
                        print("firstQuery==", firstQuery)
                        products_cursor.execute(firstQuery)
                        resVal = products_cursor.fetchone()
                        print("asinVal--->", asinVal)
                        print("resVal-->", resVal)
                        if resVal is not None:

                            product_id = resVal[0]
                            amazon_upload_selling_price = float(resVal[1])

                            logger.info(
                                "amazon_upload_selling_price for asin--------->{0}".format(amazon_upload_selling_price))

                            if isPriceUpdated == "False":
                                """
                                Update MRP Price
                                """
                                products_cursor.execute(
                                    "SELECT product_mrp from master_masterproduct WHERE product_id=" + str(product_id))
                                mrpRes = products_cursor.fetchone()
                                if mrpRes is not None:
                                    product_mrp = mrpRes[0]
                                    logger.info("product_mrp for asin--------->{0}".format(product_mrp))
                                    value_listings = [product_mrp, today]
                                    print(value_listings)

                            if len(value_listings) != 0:
                                query_write = "UPDATE amazon_amazonproducts SET amazon_competitor_lowest_price=%s, amazon_updated_at=%s WHERE amazon_unique_id= '{0}' ".format(
                                    asinVal)
                                print("query_write==", query_write)
                                products_cursor.execute(query_write, value_listings)
                                conn_products.commit()
                                current_value = float(value_listings[0])
                                buyboxVal = True if current_value > amazon_upload_selling_price else False
                                logger.info("buyboxVal for asin--------->{0}".format(buyboxVal))
                                query_write = "UPDATE master_masterproduct SET buybox=%s, updated_at=%s WHERE product_id= {0} ".format(
                                    product_id)
                                products_cursor.execute(query_write, [buyboxVal, today])
                                conn_products.commit()
                                logger.info("updated for asin--------->{0}".format(asinVal))

        products_cursor.close()
        conn_products.close()
    except Exception as e:
        logger.info("Exception----------------->{0}".format(str(e)))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(exc_type, exc_tb.tb_lineno)
        print("=================")
    finally:
        # products_cursor.close()
        # conn_products.close()
        return {'statusCode': 200, 'body': "Done"}


print(lambda_handler())