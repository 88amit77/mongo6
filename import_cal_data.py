import math
import json
import os
from datetime import date
import csv
from datetime import datetime
import datetime
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


# def lambda_handler(event, context):
def lambda_handler():
    db_name = 'postgres'
    credential = db_credential(db_name)
    print("credential====", credential)
    cred_for_sqlalchemy = credential["response"]["db_detail"]["db_detail_for_sqlalchemy"]
    print("cred_for_sqlalchemy--", cred_for_sqlalchemy)
    ##employees
    cred_for_sqlalchemy_emp = cred_for_sqlalchemy + "/employees"
    print("cred_for_sqlalchemy_emp--", cred_for_sqlalchemy_emp)

    # def lambda_handler():

    # print(event)
    # body = json.loads(event['body'])
    # print("body===")
    # print(body)
    # emp_id = body["emp_id"]
    # print(emp_id)
    # factor = body["factor"]
    # print(factor)
    # month = str(body["month"])
    # print(month)

    # month_1 = month.split('/')[0]
    # year_1 = month.split('/')[1]
    # if month_1 == '12':
    #     year_1 = str(int(year_1) + 1)
    #     month_1 = '01'
    # else:
    #     month_1 = str(int(month_1) + 1)
    # outp = year_1 + '-' + month_1 + '-07'
    # print(outp)
    # #
    # due_date = outp
    # print(due_date)
    # lop = float(body["lop"])
    # print(lop)
    # no_of_days = body["no_of_days"]
    # print(no_of_days)
    # over_time = body["over_time"]
    # print(over_time)
    # deductions = body["deductions"]
    # print(deductions)
    # reimbursements = body["reimbursements"]
    # print(reimbursements)
    ###code start
    emp_id = 151
    month = '06/2021'
    month_1 = month.split('/')[0]
    year_1 = month.split('/')[1]
    if month_1 == '12':
        year_1 = str(int(year_1) + 1)
        month_1 = '01'
    else:
        month_1 = str(int(month_1) + 1)
    outp = year_1 + '-' + month_1 + '-07'
    print(outp)

    due_date = outp
    # due_date = '2021-03-07'
    lop = 3
    no_of_days = 30
    over_time = 0
    deductions = 0
    reimbursements = 0
    try:
        engine = create_engine(cred_for_sqlalchemy_emp)
        query = "select * from api_salary where emp_id_id = " + str(emp_id)
        sql = pd.read_sql(query, engine)
        sql.to_csv('a0.csv', index=False)
        sql0=pd.read_csv('a0.csv')
        ##CTC/(CTC - (PF+EPF+PT+ESI_Emp+ESI_empl)
        #id,ctc,basic,hra,conveyance_allowances,medical_allowance,cca_allowance,pf_employer,pf_employee,pt,esi_employer,esi_employee,emp_id_id,special_allowances,pf_status,esi_eligible,internship_status

        sql0['factor'] = sql0['ctc']/(sql0['ctc']-(sql0['pf_employer']+sql0['pf_employee']+sql0['pt']+sql0['esi_employer']+sql0['esi_employee']))
        sql0.to_csv('a1.csv', index=False)
        sql1 = pd.read_csv('a1.csv')

        #CTC*((No Of Days - LOP)/No Of Days))
        sql1['ctc1'] = sql1['ctc'] * ((no_of_days-lop)/no_of_days)
        ###Basic*((No Of Days - (Factor*LOP))/No Of Days
        sql1['basic1'] = sql1['basic'] * ((no_of_days-(sql1['factor']*lop))/no_of_days)
        ###HRA*((No Of Days - (Factor*LOP))/No Of Days
        sql1['hra1'] = sql1['hra'] * ((no_of_days-(sql1['factor']*lop))/no_of_days)
        ##Conveyance*((No Of Days - (Factor*LOP))/No Of Days
        sql1['conveyance_allowances1'] = sql1['conveyance_allowances'] * ((no_of_days-(sql1['factor']*lop))/no_of_days)
        ##medical*((No Of Days - (Factor*LOP))/No Of Days
        sql1['medical_allowance1'] = sql1['medical_allowance'] * ((no_of_days-(sql1['factor']*lop))/no_of_days)
        ###CCA*((No Of Days - (Factor*LOP))/No Of Days
        sql1['cca_allowance1'] = sql1['cca_allowance'] * ((no_of_days-(sql1['factor']*lop))/no_of_days)
        ###Special*((No Of Days - (Factor*LOP))/No Of Days
        sql1['special_allowances1'] = sql1['special_allowances'] * ((no_of_days - (sql1['factor'] * lop)) / no_of_days)


        sql1['pf_employer1'] = sql1['pf_employer']
        sql1['pf_employee1'] = sql1['pf_employee']
        sql1['Gross_Salary'] = sql1['ctc1'] - float(sql1['pf_employer1']) - float(sql1['pt'])
        sql1['esi_employer1'] = sql1['esi_employer']
        sql1['esi_employee1'] = sql1['esi_employee']


        ##ctc-sum(pf_employer:esi_employee)
        sql1['net_employee_payable1'] = 1
        sql1.to_csv('a2.csv', index=False)
        sql5 = pd.read_csv('a2.csv')
        sql5['ctc'] = sql1['ctc1']
        sql5['basic'] = sql1['basic1']
        sql5['hra'] = sql1['hra1']
        sql5['conveyance_allowances'] = sql1['conveyance_allowances1']
        sql5['medical_allowance'] = sql1['medical_allowance1']
        sql5['cca_allowance'] = sql1['cca_allowance1']
        sql5['pf_employer'] = sql1['pf_employer1']
        sql5['pf_employee'] = sql1['pf_employee1']
        sql5['pt'] = sql1['pt']
        sql5['esi_employer'] = sql1['esi_employer1']
        sql5['esi_employee'] = sql1['esi_employee1']
        sql5['net_employee_payable'] = 0
        sql5['due_date'] = due_date
        sql5['special_allowances'] = sql1['special_allowances1']
        sql5['over_time'] = over_time
        sql5['deductions'] = deductions
        sql5['reimbursements'] = reimbursements
        sql5['emp_id_id'] = emp_id
        sql5['Gross_Salary'] = sql1['Gross_Salary']
        sql5.to_csv('a7.csv', index=False)
        data = pd.read_csv('a7.csv')
        data1 = data.drop(['id'], axis=1)
        data1['month'] = str(month)
        data1['lop'] = float(lop)
        data1['no_of_days'] = int(no_of_days)
        data1['net_employee_payable'] = data1['ctc'] - (data1['pf_employer']+data1['pf_employee']+data1['esi_employer']+data1['esi_employee']+data1['pt'])

        # ###added code to take value to 2 decimal place only
        data1['net_employee_payable'] = data1['net_employee_payable'].apply(lambda x: round(x, 2))
        data1['ctc'] = data1['ctc'].apply(lambda x: round(x, 2))
        data1['basic'] = data1['basic'].apply(lambda x: round(x, 2))
        data1['hra'] = data1['hra'].apply(lambda x: round(x, 2))
        data1['conveyance_allowances'] = data1['conveyance_allowances'].apply(lambda x: round(x, 2))
        data1['medical_allowance'] = data1['medical_allowance'].apply(lambda x: round(x, 2))
        data1['cca_allowance'] = data1['cca_allowance'].apply(lambda x: round(x, 2))
        data1['pf_employer'] = data1['pf_employer'].apply(lambda x: round(x, 2))
        data1['pf_employee'] = data1['pf_employee'].apply(lambda x: round(x, 2))
        data1['esi_employer'] = data1['esi_employer'].apply(lambda x: round(x, 2))
        data1['esi_employee'] = data1['esi_employee'].apply(lambda x: round(x, 2))
        data1['pt'] = data1['pt'].apply(lambda x: round(x, 2))
        data1['special_allowances'] = data1['special_allowances'].apply(lambda x: round(x, 2))
        data1['Gross_Salary'] = data1['Gross_Salary'].apply(lambda x: round(x, 2))
        # ##added for status column
        data1['status'] = 0
        data1.drop(['pf_status','esi_eligible','internship_status','factor','ctc1','basic1','hra1','conveyance_allowances1','medical_allowance1','cca_allowance1',
                    'special_allowances1','pf_employer1','pf_employee1','esi_employer1','esi_employee1','net_employee_payable1'] ,axis=1, inplace=True)
        ##added line ends

        data1.to_csv('a9.csv', index=False)
        print("true")
        if data1.empty:
            print("no data found")
            message = "No data found"
        else:
            print("data1===", data1)

            data1.to_sql(
                name='api_monthlyempsalary',
                con=engine,
                index=False,
                if_exists='append'
            )
            message = 'data upload successful'

        engine.dispose()
        return {
            'statusCode': 200,
            'Message': message

        }
    except Exception as e:
        return {
            'statusCode': 404,
            'Message': "data upload error",
            'error': {e}

        }

print(lambda_handler())
