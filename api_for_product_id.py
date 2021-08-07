import json
import os
import psycopg2
import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import psycopg2
import requests
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


def lambda_handler():
# def lambda_handler(event, context):
    db_name = 'postgres'
    credential = db_credential(db_name)
    print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##orders
    cred_for_sqlalchemy_orders = cred_for_sqlalchemy + "/orders"
    print("cred_for_sqlalchemy_orders--", cred_for_sqlalchemy_orders)
    ##employees
    cred_for_sqlalchemy_employees = cred_for_sqlalchemy + "/employees"
    print("cred_for_sqlalchemy_employees--", cred_for_sqlalchemy_employees)
    ##products
    cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
    print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)
    ##users
    cred_for_sqlalchemy_users = cred_for_sqlalchemy + "/users"
    print("cred_for_sqlalchemy_users--", cred_for_sqlalchemy_users)
    ##vendors
    cred_for_sqlalchemy_vendors = cred_for_sqlalchemy + "/vendors"
    print("cred_for_sqlalchemy_vendors--", cred_for_sqlalchemy_vendors)

    buymore_sku = 'CF_M18SS55WH4056-2XL'
    buymore_sku1 = "'" + buymore_sku + "'"
    # buymore_sku = event.get("buymore_sku", None)
    # buymore_sku1 = "'" + buymore_sku + "'"
    # print('buymore_sku1==>', buymore_sku1)
    #
    # fnsku = event.get("fnsku", None)
    # fnsku1 = "'" + fnsku + "'"
    # print('fnsku1==>', fnsku1)
    #
    # listing_id = event.get("listing_id", None)
    # listing_id1 = "'" + listing_id + "'"
    # print('listing_id1==>', listing_id1)

    fnsku = None
    listing_id = None

    if fnsku != None:
        # if fnsku1 :
        engine = create_engine(cred_for_sqlalchemy_products)
        query = "select amazon_listing_id from amazon_amazonproducts where amazon_listing_id = " + str(fnsku1)

        # + str(bin_id)
        print("query==", query)
        sql = pd.read_sql(query, engine)
        engine.dispose()
        print("sql", sql)
        sql.to_csv('/tmp/z3.csv', index=False)
        data = pd.read_csv('/tmp/z3.csv')
        product_id = data['product_id'][0]
        if data.empty == True:
            status = 'False'
        else:
            status = 'True'
    elif listing_id != None:
        # elif listing_id1:
        engine = create_engine(cred_for_sqlalchemy_products)
        query = "select flipkart_listing_id from flipkart_flipkartproducts where flipkart_listing_id = " + str(
            listing_id1)

        # + str(bin_id)
        print("query==", query)
        sql = pd.read_sql(query, engine)
        engine.dispose()
        print("sql", sql)
        sql.to_csv('/tmp/z4.csv', index=False)
        data = pd.read_csv('/tmp/z4.csv')
        product_id = data['product_id'][0]
        if data.empty == True:
            status = 'False'
        else:
            status = 'True'
    elif buymore_sku != None:
        # elif listing_id1:
        engine = create_engine(cred_for_sqlalchemy_products)
        query = "select product_id from master_masterproduct where buymore_sku = " + str(
            buymore_sku1)

        # + str(bin_id)
        print("query==", query)
        sql = pd.read_sql(query, engine)
        engine.dispose()
        print("sql", sql)
        sql.to_csv('/tmp/z4.csv', index=False)
        data = pd.read_csv('/tmp/z4.csv')
        product_id=data['product_id'][0]
        if data.empty == True:
            status = 'False'
        else:
            status = 'True'
    else:
        status = "fnsku or listing_id or buymore_sku required"
        product_id = ''

    return {
        'statusCode': 200,
        'status': status,
        'product_id': product_id,

    }


print(lambda_handler())