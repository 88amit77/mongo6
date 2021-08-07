import datetime
import io
import os
import sys
# from io import StringIO
import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

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


# def lambda_handler(event, context):
def lambda_handler():
    # ean = event.get("ean", None)
    ean='_8901188622669'
    db_name = 'postgres'
    credential = db_credential(db_name)
    print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    # print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##orders
    cred_for_sqlalchemy_users = cred_for_sqlalchemy + "/users"
    ##products
    cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"

    try:
        if ean is None:
            message = "ean no required"

        else:
            engine = create_engine(cred_for_sqlalchemy_products)
            query = "select product_id from master_masterproduct where ean = '" + str(ean) +"'"
            print("query==", query)
            d_last = pd.read_sql(query, engine)
            d_last.to_csv('/tmp/z.csv', index=False)
            d_last1 = pd.read_csv('/tmp/z.csv')
            product_id=d_last1['product_id'][0]
            #for amazon
            query_amazon = "select product_id,amazon_listing_id from amazon_amazonproducts where product_id = "+str(product_id)
            amazon = pd.read_sql(query_amazon, engine)
            amazon.to_csv('/tmp/z1.csv', index=False)
            df_amazon=pd.read_csv('/tmp/z1.csv')
            if df_amazon['amazon_listing_id'].empty:
                pass
            else:
                data=pd.merge(left=d_last1,right=df_amazon,left_on='product_id',right_on='product_id')
                data.to_csv('/tmp/z2.csv',index=False)
            ####for flipkart
            query_flipkart = "select product_id,flipkart_listing_id from flipkart_flipkartproducts where product_id = " + str(
                product_id)
            flipkart = pd.read_sql(query_flipkart, engine)
            flipkart.to_csv('/tmp/z1.csv', index=False)
            df_flipkart = pd.read_csv('/tmp/z1.csv')
            if df_flipkart['flipkart_listing_id'].empty:
                pass
            else:
                data = pd.merge(left=d_last1, right=df_flipkart, left_on='product_id', right_on='product_id')
                data.to_csv('/tmp/z2.csv', index=False)
            d_last1 = pd.read_csv('/tmp/z2.csv')
            j = d_last1.to_json(orient='records')
            data_data = j
            print("data_data==", data_data)
            message = "ok"

        return {
            'statusCode': 200,
            'message': message,
            'category': json.loads(data_data)
        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "some error",
            'error': {e}
        }

print(lambda_handler())