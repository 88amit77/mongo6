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
from datetime import date


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

    # user_id = event.get("user_id", None)
    user_id = 1324
    today = str(date.today())
    print("Today's date:", today)
    start_date1 = today
    start_date = "'" + start_date1 + "'"
    print("start_date==", start_date)
    end_date1 = today+' 23:59:59'
    end_date = "'" + end_date1 + "'"
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
        if user_id is None:
            message = "user_id required"

        else:
            engine = create_engine(cred_for_sqlalchemy_employees)
            query = "select * from public.api_kanbanflowtime where stop_time is Null and user_id = " + str(
                user_id) + " and start_time BETWEEN "+str(start_date)+" AND "+str(end_date)
            #query = "select * from public.api_kanbanflowtime where stop_time is Null and user_id = 4 and  start_time BETWEEN '2021-06-30' AND '2021-06-30 23:59:59'"

            print("query==", query)
            sql = pd.read_sql(query, engine)
            print(sql)
            sql.to_csv('a2.csv', index=False)
            df=pd.read_csv('a2.csv')
            # marks_list = sql['emp_id_id'].tolist()

            # d_last.to_csv('/tmp/z.csv')
            j = df.to_json(orient='records')
            data_data = j

        return {
            'statusCode': 200,
            'result': json.loads(j)

        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "some error",
            'error': {e}
        }

print(lambda_handler())