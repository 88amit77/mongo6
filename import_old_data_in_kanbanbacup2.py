import datetime
import io
import os
import sys
# from io import StringIO
import csv
import requests
# import dropbox
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
    # print(event)
    db_name = 'postgres'
    credential = db_credential(db_name)
    print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##products
    cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
    print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)

    ##employees
    cred_for_sqlalchemy_employees = cred_for_sqlalchemy + "/employees"
    print("cred_for_sqlalchemy_employees--", cred_for_sqlalchemy_employees)
    emp_id = 151
    month = '06/2021'
    if emp_id is None and month is None:
        statusCode = 200
        final_data = 'emp_id or month field required'
    else:
        engine = create_engine(cred_for_sqlalchemy_employees)
        query0 = "select emp_id,over_time,deductions,reimbursements from api_employee_deductions where emp_id ="+str(emp_id)+"and month="+"'"+str(month)+"'"
        data = pd.read_sql(query0, engine)
        data.to_csv('/tmp/z1.csv')
        data2=pd.read_csv('/tmp/z1.csv')
        data4 = data2.groupby(['emp_id'])['over_time', 'deductions', 'reimbursements'].sum()
        data4.to_csv('z2.csv')
        data5=pd.read_csv('z2.csv')
        # print("data4==>",data4)
        j = data5.to_json(orient='records')
        data_data = j
        statusCode = 200
        final_data=json.loads(data_data)
        engine.dispose()
    return {
        'statusCode': statusCode,
        'result':final_data
              }


print(lambda_handler())