import json
import os
import pandas as pd
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


# def lambda_handler(event, context):
def lambda_handler():
    # print(event)
    db_name = 'postgres'
    credential = db_credential(db_name)
    #print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    #print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##orders
    cred_for_sqlalchemy_orders = cred_for_sqlalchemy + "/orders"
    #print("cred_for_sqlalchemy_orders--", cred_for_sqlalchemy_orders)
    ##employees
    cred_for_sqlalchemy_employees = cred_for_sqlalchemy + "/employees"
    #print("cred_for_sqlalchemy_employees--", cred_for_sqlalchemy_employees)
    ##products
    cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
    #print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)
    ##users
    cred_for_sqlalchemy_users = cred_for_sqlalchemy + "/users"
    #print("cred_for_sqlalchemy_users--", cred_for_sqlalchemy_users)
    ##vendors
    cred_for_sqlalchemy_vendors = cred_for_sqlalchemy + "/vendors"
    #print("cred_for_sqlalchemy_vendors--", cred_for_sqlalchemy_vendors)


    # email = event.get("email", None)
    email = 'ketan@uessentials.com'
    if email is None :
        statusCode = 404
        final_data = 'email field required'

    else:
        engine = create_engine(cred_for_sqlalchemy_users)
        # query = "select * from api_kanbanflowtime where user_id ="+str(user_id)+" and start_time = " + "'"+str(start_time)+ "'"
        query = "select * from auth_user where email in ('"+str(email)+"')"
        #print("query==", query)
        sql = pd.read_sql(query, engine)
        sql.to_csv('/tmp/a2.csv', index=False)
        sql1 = pd.read_csv('/tmp/a2.csv')
        j = sql1.to_json(orient='records')
        data_data = j
        statusCode = 200
        final_data = json.loads(data_data)
        engine.dispose()

    return {
        'statusCode': statusCode,
        'result': final_data

    }
print(lambda_handler())