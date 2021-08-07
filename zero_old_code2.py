import json
import os
import psycopg2
import dropbox
from datetime import date
import csv
from datetime import datetime
import datetime
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine


def lambda_handler(event, context):
    # def lambda_handler():
    print("event==")
    print(event)
    body = json.loads(event['body'])
    print("body===")
    print(body)

    month1 = body["month"]

    # start_date1 = '2021-02-01'
    print('start_date1', month1)
    month2 = "'" + month1 + "'"
    print('month2', month2)

    # engine = create_engine(
    #     'postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/employees')
    engine = create_engine(
        'postgresql://postgres:r2DfZEyyNL2VLfg2@buymore-dev-aurora.cluster-cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/employees')
    # query = "SELECT api_monthlyempsalary.id,api_employee.name,api_monthlyempsalary.net_employee_payable,api_monthlyempsalary.month FROM api_employee, api_monthlyempsalary where api_employee.emp_id = api_monthlyempsalary.emp_id_id and api_monthlyempsalary.status = 0 and api_monthlyempsalary.month = "+ "'" + str(month1) + "'"
    # query = query = 'SELECT api_monthlyempsalary.id,api_monthlyempsalary.emp_id_id,api_employee.name,api_monthlyempsalary.lop,api_monthlyempsalary.no_of_days,api_monthlyempsalary.ctc,' \
    #         'api_monthlyempsalary.basic,api_monthlyempsalary.hra,api_monthlyempsalary.conveyance_allowances,api_monthlyempsalary.medical_allowance,api_monthlyempsalary.cca_allowance,' \
    #         'api_monthlyempsalary.pf_employer,' \
    #         'api_monthlyempsalary.pf_employee, ' \
    #         'api_monthlyempsalary.pt,api_monthlyempsalary.esi_employer,api_monthlyempsalary.esi_employee,api_monthlyempsalary.net_employee_payable,api_monthlyempsalary.due_date,api_monthlyempsalary.special_allowances,api_monthlyempsalary.over_time,deductions, ' \
    #         'api_monthlyempsalary.reimbursements,api_monthlyempsalary."Gross_Salary",api_monthlyempsalary.status ' \
    #         'FROM api_employee, api_monthlyempsalary where api_employee.emp_id = api_monthlyempsalary.emp_id_id and api_monthlyempsalary.status = 0 and api_monthlyempsalary.month = '+"'"+str(month1)+"'"
    query = "select * from api_monthlyempsalary  where emp_id_id = 158"
    # print("query==",query)
    sql = pd.read_sql(query, engine)
    sql.to_csv('/tmp/a2.csv', index=False)
    marks_list = sql['emp_id_id'].tolist()

    ###
    # connection = engine.connect()
    # update_query = 'update api_monthlyempsalary set status = 4 where emp_id_id = 158'
    # connection.execute(update_query)

    # d_last.to_csv('/tmp/z.csv')
    # j = sql.to_json(orient='records')
    # data_data = j
    # headers1=list(sql.columns)
    header = {
        "id": "ID",
        "emp_id_id": "EMP ID",
        "name": "NAME",
        "lop": "LOP",
        "no_of_days": "NO OF DAYS",
        "ctc": "CTC",
        "basic": "BASIC",
        "hra": "HRA",
        "conveyance_allowances": "CONVEYANCE ALLOWANCE",
        "medical_allowance": "MEDICAL ALLOWANCE",
        "cca_allowance": "CCA ALLOWANCE",
        "pf_employer": "PF EMPLOYER",
        "pf_employee": "PF EMPLOYEE",
        "pt": "PT",
        "esi_employer": "ESI EMPLOYER",
        "esi_employee": "ESI EMPLOYEE",
        "net_employee_payable": "NET EMPLOYEE PAYABLE",
        "due_date": "DUE DATE",
        "special_allowances": "SPECIAL ALLOWANCE",
        "over_time": "OVER TIME",
        "deductions": "DEDUCTIONS",
        "reimbursements": "REIMBURSEMENTS",
        "Gross_Salary": "GROSS SALARY",
        "status": "STATUS"
    }
    # headers = ['ID','Name','Net Employee Payable','Month']
    engine.dispose()

    return {
        'status': True,
        'marks_list':marks_list,
        # 'header': header
        # 'result': json.loads(j),
        # 'result': marks_list
    }

# print(lambda_handler())