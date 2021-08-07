import csv

import mws, math, psycopg2, json
import sys, os
from datetime import datetime, timedelta
import logging
import requests
from sqlalchemy import create_engine


# from throttle import throttle
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


def find_productId(asin):
    db_name = 'postgres'
    credential = db_credential(db_name)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    ##orders
    cred_for_sqlalchemy_orders = cred_for_sqlalchemy + "/orders"
    ##employees
    cred_for_sqlalchemy_employees = cred_for_sqlalchemy + "/employees"
    ##products
    cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
    ##users
    cred_for_sqlalchemy_users = cred_for_sqlalchemy + "/users"
    ##vendors
    cred_for_sqlalchemy_vendors = cred_for_sqlalchemy + "/vendors"
    ###psycopg2 connection
    cred_for_psycopg2 = credential["response"]["db_detail"]["db_detail_for_psycopg2"]
    rds_host = cred_for_psycopg2['endPoint']
    name = cred_for_psycopg2['userName']
    password = cred_for_psycopg2['passWord']
    ##products
    db_name = "products"

    rds_host = rds_host
    name = name
    password = password
    db_name = db_name
    conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
    # print('connected')
    #
    qry = "select product_id from amazon_amazonproducts where amazon_unique_id= '"+asin+"'"
    cur = conn.cursor()
    cur.execute(qry)
    conn.commit()
    # print(qry)
    result = cur.fetchall()
    if len(result)==0:
        return 'product id not found'
    pid=result[0][0]
    # print('result:',pid)
    return pid

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



# def lambda_handler(event, context):
def lambda_handler():
    try:
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
        ##products
        cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
        ##users
        cred_for_sqlalchemy_users = cred_for_sqlalchemy + "/users"
        ##vendors
        cred_for_sqlalchemy_vendors = cred_for_sqlalchemy + "/vendors"
        ###psycopg2 connection
        cred_for_psycopg2 = credential["response"]["db_detail"]["db_detail_for_psycopg2"]
        rds_host = cred_for_psycopg2['endPoint']
        name = cred_for_psycopg2['userName']
        password = cred_for_psycopg2['passWord']
        ##products
        db_name = "products"

        account = accounts[0]
        # productsMWS = mws.Products(access_key=account['ACCESS_KEY'], secret_key=account['SECRET_KEY'],
        #                            account_id=account['SELLER_ID'], region='IN')
        productsMWS = mws.Products(access_key="AKIAJYQAC3UP6RYQELXQ", secret_key="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",
                                   account_id="A1JMG6531Z3EIJ", region="IN")
        print("productsMWS===>",productsMWS)
        RDS_HOST = rds_host
        NAME = name
        PASSWORD = password
        DB_NAME = db_name

        conn_products = psycopg2.connect(host=RDS_HOST, database=DB_NAME, user=NAME, password=PASSWORD)
        products_cursor = conn_products.cursor()
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # if event.get("Records", None) is not None:
        #     data = json.loads(event["Records"][0]["body"])
        # else:
        #     data = event

        # asin_list = data.get("asin_list", None)
        ###amazon data base 4000 limit sqs trigger
        asin_list = ['B07RBR1FRX','B07RWJBV2N','B07RWK9GJ4','B07RWZH8PY']
        # asin_list = ['B07GX6VWML']
        if asin_list is not None:
            try:
                data = productsMWS.get_matching_product(marketplaceid='A21TJRUUN4KGV',asins=asin_list)
                #marketplaceids
                print("data--",data)
                dataList = data.parsed
                print("dataList-->>",dataList)
            except TypeError as e:
                print("-------" + str(e))
                dataList = []
            # ###empty data frame
            # column_names = ["product_id", "sales_rank"]
            #
            # df = pd.DataFrame(columns=column_names)
            if len(dataList) != 0:
                for singleResult in dataList:
                    try:
                        # print("singleResult==",singleResult)
                        # asin=singleResult['ASIN']['value']
                        # sp_for_rank=str(singleResult).replace("'",'"')
                        sp_for_rank=json.dumps(singleResult)
                        sp_for_rank=json.loads(sp_for_rank)
                        asin=sp_for_rank['ASIN']['value']
                        # print('asin==>', asin, end='    ')
                        # print('asin==>', asin)
                        try:
                            sales_rank_value=sp_for_rank['Product']['SalesRankings']['SalesRank'][0]['Rank']['value']
                            # print('sales_rank_value==d=>',sales_rank_value,end=' ')
                            print('sales_rank_value===>', sales_rank_value)
                            pid=find_productId(asin)
                            print('product_id===>',pid)
                            ###update query
                            ##UPDATE table
                            engine = create_engine(cred_for_sqlalchemy_products)
                            connection = engine.connect()
                            update_query = "update master_masterproduct SET sales_rank = "+str(sales_rank_value)+" WHERE product_id = "+str(pid)
                            print("update_query=>",update_query)
                            connection.execute(update_query)

                        except:
                            pass

                        # updatesalerank(pid,sales_rank_value)
                    except KeyError as e:
                        print('singleResult skipped bcz of key error')


    except Exception as e:
        logger.info("Exception----------------->{0}".format(str(e)))
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print(exc_type, exc_tb.tb_lineno)
        print("=================")
    finally:
        products_cursor.close()
        conn_products.close()
        return {'statusCode': 200, 'body': "Done"}
print(lambda_handler())




