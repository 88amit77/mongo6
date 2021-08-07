import json
import os

import pandas as pd
import numpy as np

from sqlalchemy import create_engine
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



def lambda_handler(event, context):
# def lambda_handler():
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



    engine1 = create_engine(cred_for_sqlalchemy_users)
    query1 = 'select id from auth_user'
    sql = pd.read_sql(query1, engine1)
    sql.to_csv('/tmp/z2.csv', index=False)

    engine2 = create_engine(cred_for_sqlalchemy_vendors)
    query2 = 'select user_id,marketing_incharge_id from  api_newvendordetails'
    sql = pd.read_sql(query2, engine2)
    sql.to_csv('/tmp/z3.csv', index=False)
    df11 = pd.read_csv("/tmp/z3.csv")
    df12 = pd.read_csv("/tmp/z2.csv")
    df22 = pd.merge(left=df12, right=df11, left_on='id', right_on='user_id')
    df22.to_csv('/tmp/z4.csv', index=False)

    engine3 = create_engine(cred_for_sqlalchemy_employees)
    query3 = 'select emp_id,name from api_employee'
    sql = pd.read_sql(query3, engine3)
    sql.to_csv('/tmp/z5.csv', index=False)

    df111 = pd.read_csv("/tmp/z4.csv")
    df121 = pd.read_csv("/tmp/z5.csv")
    df221 = pd.merge(left=df111, right=df121, left_on='marketing_incharge_id', right_on='emp_id')
    df221.to_csv('/tmp/z6.csv', index=False)
    # df_final = df221.drop(['id','marketing_incharge_id','emp_id'], axis=1)
    df_final = df221.drop(['user_id', 'id', 'marketing_incharge_id'], axis=1)
    df_final1 = df_final.drop_duplicates(subset="emp_id", keep='last')
    df_final1.to_csv('/tmp/z7.csv', index=False)
    j = df_final1.to_json(orient='records')
    data_data = j





    engine1.dispose()
    engine2.dispose()
    engine3.dispose()

    return {
        'status': True,
        'result': json.loads(data_data)

    }

# print(lambda_handler())