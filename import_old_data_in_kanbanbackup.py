import datetime
import io
import os
import sys
# from io import StringIO
import csv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import collections

import json
import requests
from datetime import date, timedelta


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
    today = str(date.today())
    ##to get 48 hours back date
    final_date = str(date.today() - timedelta(hours=48, minutes=00))
    print("final_date", final_date)
    # data_time = today - timedelta(hours=3)
    # print("data_time",data_time)
    print("final_date date:", final_date)

    final_date2 = "'" + final_date + ' 23:59:59' + "'"
    print("final_date2==", final_date2)
    final_date4 = final_date2

    print("final_date4", final_date4)
    db_name = 'postgres'
    credential = db_credential(db_name)
    print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##employees
    cred_for_sqlalchemy_employees = cred_for_sqlalchemy + "/employees"
    print("cred_for_sqlalchemy_employees--", cred_for_sqlalchemy_employees)

    # category = "Orders & Returns"

    try:

        engine = create_engine(cred_for_sqlalchemy_employees)
        query = "select * from api_kanbantask where status =1 and updated_at <" + str(final_date4)
        print("query==", query)
        sql = pd.read_sql(query, engine)
        sql.to_csv('a0.csv', index=False)
        ###to import data in regular task backup tables
        df2=pd.read_csv('a0.csv')
        df2['members']=df2['members'].astype(str)
        df4 = df2.drop(['regular_task_id'], axis=1)
        df4.to_csv('a4.csv', index=False)
        # df2.dropna(['regular_task_id'],axis=1)

        df4.to_sql(
            name='api_kanbantaskbackup',
            con=engine,
            index=False,
            if_exists='append'
        )

        ##now to delete old  data from kanban regual task table
        df1 = pd.read_csv('a0.csv')
        regular_task_id = df1["regular_task_id"].tolist()
        w0 = [str(elem) for elem in regular_task_id]
        w1 = sorted(set(w0))
        w2 = str(w1)
        regular_task_ids = w2[1:-1]
        print("regular_task_ids----", regular_task_ids)
        ##delete query
        connection = engine.connect()
        delete_query = "DELETE FROM api_kanbantask where regular_task_id in (" + str(regular_task_ids) + ")"
        print("delete_query", delete_query)
        connection.execute(delete_query)

        return {
            'statusCode': 200,
            'Message': "data moved to kanban replication table",

        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "some error",
            'error': {e}
        }


print(lambda_handler())

