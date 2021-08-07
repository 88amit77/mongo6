import json
import os
import psycopg2
import dropbox
from datetime import date
from datetime import datetime
from datetime import datetime
import csv
from datetime import datetime
from datetime import datetime as dt
import datetime
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import requests
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import calendar
import datetime
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
    # print("today-->",today)
    start_datetime = "'" + today+ ' 07:00:00' + "'"
    end_datetime = "'" + today + ' 23:59:59' + "'"
    print("start_datetime==", start_datetime)
    print("end_datetime==", end_datetime)

    db_name = 'postgres'
    credential = db_credential(db_name)
    # print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    # print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##employees
    cred_for_sqlalchemy_emp = cred_for_sqlalchemy + "/employees"
    # print("cred_for_sqlalchemy_emp--", cred_for_sqlalchemy_emp)

    # department_name = event.get("department_name", None)
    department_name = 'TechTeam'
    if department_name != None:

        try:
            ##database connection
            engine = create_engine(cred_for_sqlalchemy_emp)


            query4="select * from public.api_kanbantask as a Right join public.api_kanbansubtask as b on a.regular_task_id = b.sub_tasks_id where a.task_department_name = '"+str(department_name)+"' and a.created_at between "+str(start_datetime)+" and "+str(end_datetime)
            print("query4=>",query4)
            sql4 = pd.read_sql(query4, engine)
            sql4.to_csv('/tmp/a40.csv', index=False)
            df4 = pd.read_csv('/tmp/a40.csv')
            df4['status'] = df4['status'].astype(str)
            df4.loc[df4['status'].str.contains('1'), 'status'] = 'Done'
            df4.loc[df4['status'].str.contains('2'), 'status'] = 'In Progress'
            df4.loc[df4['status'].str.contains('3'), 'status'] = 'On hold'
            df4.loc[df4['status'].str.contains('4'), 'status'] = 'Assigned'
            df4.rename(columns={'created_at.1': 'created_at1'}, inplace=True)
            df4.rename(columns={'updated_at.1': 'updated_at1'}, inplace=True)

            df4['created_at'] = pd.to_datetime(df4.created_at).dt.strftime('%d-%m-%Y %H:%M:%S')
            df4['updated_at'] = pd.to_datetime(df4.updated_at).dt.strftime('%d-%m-%Y %H:%M:%S')
            df44 = df4.drop(['created_at1', 'updated_at1', 'sub_tasks_id','regular_task_id.1'], axis=1)
            ##for proper ordering
            df444 = df44[['regular_task_id','task_name','task_department_name','task_type','members','task_description','task_files','task_due_date','cron','previous_regular_task_id','status','knowledge_center','sub_task_id','sub_task_name','sub_task_value','sub_task_path','created_at','updated_at']]
            df444.to_csv('/tmp/a41.csv', index=False)
            ###dropbox settings
            today_date=datetime.datetime.today().strftime('%d-%m-%Y')
            file_name = str(department_name) + "-" + str(today_date) + '.csv'
            print('fn', file_name)

            file_from = '/tmp/' + file_name

            inputfile = '/tmp/a41.csv'
            with open(inputfile, newline='') as f:
                reader = csv.reader(f)
                data4 = list(reader)
            print("data4==", data4)

            with open(file_from, 'w', newline='') as file:
                writer = csv.writer(file)
                # writer.writerows(a2)
                writer.writerows(data4)
            #
            file_to = '/buymore2/Sign_Off_Report/' + today_date + "/" + department_name + "/" + file_name
            print('ft=>', file_to)
            access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
            dbx = dropbox.Dropbox(access_token)

            with open(file_from, 'rb') as f:
                dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)



            engine.dispose()
            return {
                'statusCode': 200,
                'Message': "report saved in the dropbox",

            }

        except Exception as e:
            return {
                'statusCode': 404,
                'Message': "some error",
                'error': {e}
            }
    else:
        return {
            'statusCode': 404,
            'Message': "task_department_name field required",

        }



print(lambda_handler())
