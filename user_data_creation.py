# insert_query= "INSERT INTO auth_user (username, email, password ) VALUES ("+str(username)+", "+str(email)+", "+str(password1)+")"
# insert_query = "INSERT INTO auth_user (username, email, password,is_superuser,first_name,last_name,is_staff,is_active,date_joined) VALUES ('" + str(
#     username) + "' , '" + str(email) + "','" + str(
#     password1) + "',False,'','',False,True,'2020-09-04 16:26:26.089431+00')"
# print("insert_query==>", insert_query)


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
from validate_email import validate_email



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

    db_name = 'postgres'
    credential = db_credential(db_name)
    print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##products
    cred_for_sqlalchemy_products = cred_for_sqlalchemy + "/products"
    print("cred_for_sqlalchemy_products--", cred_for_sqlalchemy_products)
    cred_for_sqlalchemy_users = cred_for_sqlalchemy + "/users"
    print("cred_for_sqlalchemy_users--", cred_for_sqlalchemy_users)

    # username = event.get("username", None)
    # email = event.get("email", None)
    # password = event.get("password", None)
    #ascii_password = ord(password)
    # is_superuser = event.get("is_superuser", None)
    # first_name = event.get("first_name", None)
    # last_name = event.get("last_name", None)
    # is_staff = event.get("is_staff", None)
    # is_active = event.get("is_active", None)
    # date_joined = event.get("date_joined", None)
    ##hard coded
    username = 'amit_test2'
    email = 'amittest@gmail.com'
    password = '12345'
    # ascii_password = ord(str(password))
    is_superuser = False
    first_name = 'amit'
    last_name = 'test'
    is_staff = False
    is_active = True
    date_joined = '2020-09-04 16:26:26.089431+00'
    is_valid = validate_email(email)
    print(is_valid)
    if is_valid == True:
        try:
            print("inside try")


            data = pd.DataFrame(columns=['username', 'email', 'password','is_superuser','first_name','last_name','is_staff','is_active','date_joined'])

            data.to_csv('z1.csv',index=False)

            data1=pd.read_csv('z1.csv')
            data1.iloc[0:0]
            data1.to_csv('z2.csv',index=False)
            data2=pd.read_csv('z2.csv')
            print(username)
            data2.loc['username'] = username
            data2['email'] = email
            data2['password'] = password
            data2['is_superuser'] = is_superuser
            data2['first_name'] = first_name
            data2['last_name'] = last_name
            data2['is_staff'] = is_staff
            data2['is_active'] = is_active
            data2['date_joined'] = date_joined

            data2.to_csv('z4.csv', index=False)
            data4=pd.read_csv('z4.csv')

            engine = create_engine(cred_for_sqlalchemy_users)
            data4.to_sql(
                name='auth_user',
                con=engine,
                index=False,
                if_exists='append'
            )

            engine = create_engine(cred_for_sqlalchemy_users)
            query="select id,email from auth_user where email = '"+str(email)+"' and username = '"+str(username)+"'"
            sql = pd.read_sql(query, engine)
            sql.to_csv('/tmp/z1.csv')
            j = sql.to_json(orient='records')
            data_data = j
            # print("data_data==", data_data)
            # engine.dispose()
            return {
                'statusCode': 200,
                'Message': "User data created in user table ",
                'data': json.loads(data_data)
            }
        except Exception as e:
            return {
                'statusCode': 404,
                'Message': "File upload error",
                'error': {e}
            }
    else:
        return {
            'statusCode': 404,
            'Message': "Email is not valid",
        }


print(lambda_handler())