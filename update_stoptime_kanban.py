import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import json
import requests
from datetime import date,timedelta,time



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
    ###psycopg2 connection
    cred_for_psycopg2 = credential["response"]["db_detail"]["db_detail_for_psycopg2"]
    print("cred_for_psycopg2--", cred_for_psycopg2)
    rds_host = cred_for_psycopg2['endPoint']
    name = cred_for_psycopg2['userName']
    password = cred_for_psycopg2['passWord']
    ##products
    db_name = "employees"




    try:

        engine = create_engine(cred_for_sqlalchemy_employees)
        query = "select flow_time_id,start_time,stop_time from api_kanbanflowtime where start_time is NOT Null and stop_time is Null"
        print("query==", query)
        sql = pd.read_sql(query, engine)
        sql.to_csv('a0.csv', index=False)
        df=pd.read_csv('a0.csv')
        ##
        df['stop_time'] = pd.to_datetime(df['start_time'].astype(str)) + pd.DateOffset(hours=0, minutes=30)
        df.to_csv('a1.csv',index=False)
        df1=pd.read_csv('a1.csv')
        df1.drop(['start_time'], axis=1, inplace=True)
        df1.to_csv('a2.csv',index=False)
        ###update stop time query
        df10 = pd.read_csv("a2.csv")
        iters = df10.iterrows()
        ##mod7
        for index, row in iters:
            conn = psycopg2.connect(host=rds_host,
                                    database=db_name,
                                    user=name,
                                    password=password)
            cur = conn.cursor()
            cur.execute(
                'UPDATE "api_kanbanflowtime" SET "stop_time" = %s WHERE "api_kanbanflowtime"."flow_time_id" = %s',
                [row['stop_time'],
                 row['flow_time_id']])
            conn.commit()
            conn.close()

        return {
            'statusCode': 200,
            'Message': "stop time modified",


        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "some error",
            'error': {e}
        }

print(lambda_handler())