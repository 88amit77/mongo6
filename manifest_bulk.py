import json
import os
import psycopg2
# import dropbox
import csv
from datetime import datetime
from datetime import date
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import numpy as np
import requests
import datetime

rds_host = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
name = "postgres"
password = "buymore2"
##mod1
db_name = "products"


# def lambda_handler(event, context):
def lambda_handler():
    # print(event)
    # body = json.loads(event['body'])
    # # print("body===")
    # # print(body)
    # file = body["files"]
    # print(file)
    try:
        # url = "https://content.dropboxapi.com/apitl/1/AxZJKui4Xl2BSis4-7zLIooKit2HBaYfBVy-iU3gWZ0TFYDz9AYJh7xzr4V8UXveU-XQab1l-UloueaWAWpbFBl49eDXrxB5yeq5z4k92vrsukUegAEt1qmOOXuvglZGPC2bONoecMquOmESUKMsQXFa4iiVxFnXBkotLGrnvHhV7Qm9duos82ecHbuZC8Mc60AUzJnfnIqT9ca5aFMAXCvw-KvJ7A6WmCLEL-DEvaXSiykLXvN7b5uyKyHUhbOLAMhS1IfX7Ibq2hjE6w1rqCaXcrD9nvgCZeK3FcMppAN4HwBWCnVeiJ5i6D0fuI2plndhufSioCgRXan57OB9p4KMjGM91WwpUJfa_HMjSSyAUNn5EfmYT1Zpv0YSIfPDPf0"
        # response = requests.get(url)
        #
        # with open('/tmp/out.csv', 'w') as f:
        #     writer = csv.writer(f)
        #     for line in response.iter_lines():
        #         writer.writerow(line.decode('utf-8').split(','))
        data1 = pd.read_csv('z1.csv')
        ###to convert float value to integer and NA value to '0'
        data1['warehouse_id'] = data1['warehouse_id'].fillna(0).astype(np.int64)
        data1['mf_id'] = data1['mf_id'].fillna(0).astype(np.int64)
        # data1 = pd.read_csv('z20.csv')
        data1.to_csv('/tmp/z10.csv', index=False)
        ddd = pd.read_csv('/tmp/z10.csv')

        ddd2 = ddd.dropna(subset=['order_id', 'warehouse_id'])
        ##to change column name
        ddd2.rename(columns={'mf_id': 'mf_id_id'}, inplace=True)
        ddd2.to_csv('/tmp/a0.csv', index=False)
        ##main logic to extract full data which is matching to user's entered product_id
        order_id_list = ddd2["order_id"].tolist()
        o1 = [str(elem) for elem in order_id_list]
        o2 = str(o1)
        order_ids = o2[1:-1]
        # print("order_ids----", order_ids)
        ##for warehouse
        warehouse_id_list = ddd2["warehouse_id"].tolist()
        w0 = [str(elem) for elem in warehouse_id_list]
        w1 = sorted(set(w0))
        w2 = str(w1)
        warehouse_ids = w2[1:-1]
        print("warehouse_ids----", warehouse_ids)

        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/orders')
        # query = "select dd_id,order_id,warehouse_id from api_neworder where order_id in (" +str(order_ids)+ ") and warehouse_id in ("+str(warehouse_ids)+")"

        query = "select dd_id,order_id,warehouse_id from api_neworder Inner join api_dispatchdetails ON api_neworder.dd_id = api_dispatchdetails.dd_id_id where order_id in (" + str(
            order_ids) + ") and warehouse_id in (" + str(warehouse_ids) + ") and mf_id_id is Null"

        # print(query)
        sql = pd.read_sql(query, engine)
        sql.to_csv('/tmp/a1.csv', index=False)

        df11 = pd.read_csv("/tmp/a0.csv")
        df12 = pd.read_csv("/tmp/a1.csv")
        df22 = pd.merge(left=df11, right=df12, left_on=['order_id', 'warehouse_id'],
                        right_on=['order_id', 'warehouse_id'])
        df22.rename(columns={'dd_id': 'dd_id_id'}, inplace=True)
        df22.to_csv('/tmp/a2.csv', index=False)
        df230 = pd.read_csv('/tmp/a2.csv')
        df23 = df230.drop(['order_id', 'warehouse_id'], axis=1)

        # ####import data in temp db
        engine = create_engine(
            'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/orders')
        df23.to_sql(
            name='manifest_bulk_update',
            con=engine,
            index=False,
            if_exists='append'
        )
        #
        # # #####update query
        connection = engine.connect()
        update_query = 'update api_dispatchdetails set awb = manifest_bulk_update.awb,mf_id_id = manifest_bulk_update.mf_id_id from manifest_bulk_update where api_dispatchdetails.dd_id_id = manifest_bulk_update.dd_id_id'
        connection.execute(update_query)
        # # ####query to delete temp table data
        delete_query = 'DELETE FROM manifest_bulk_update'
        connection.execute(delete_query)
        # #
        # # engine.dispose()

        engine.dispose()

        ##mod8
        return {
            'statusCode': 200,
            'Message': "File upload successful"
        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "File upload error",
            'error': {e}
        }

print(lambda_handler())