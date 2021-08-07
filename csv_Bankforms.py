import json
import pandas as pd
from sqlalchemy import create_engine
import requests
import dropbox
import csv

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

    month = '06/2021'
    # bank = 'YES_Bank'
    bank = 'NON_YES_Bank'
    ##yes bank data
    if bank == 'YES_Bank':
        engine = create_engine(cred_for_sqlalchemy_emp)
        query1 = "SELECT api_employee.name,api_employee.bank_acc_number,api_employee.ifsc_code,api_monthlyempsalary.net_employee_payable " \
                 "FROM api_employee, api_monthlyempsalary " \
                 "where api_employee.ifsc_code LIKE 'YES%%' and api_employee.emp_id = api_monthlyempsalary.emp_id_id and status = 2 and month = '" + str(
            month) + "'"
        print("query==", query1)
        sql = pd.read_sql(query1, engine)
        sql.to_csv('a2.csv', index=False)
        cc4455_final1 = pd.read_csv("a2.csv")
        # print("cc4455_final", cc4455_final)

        engine.dispose()
        data = [[
            'Credit Ac Number',
            'Beneficary name',
            'Amount',
        ]]

        for index, d in cc4455_final1.iterrows():
            # for index, d in cc1.iterrows():
            print("d", d)
            data.append([

                d[1],
                d[0],
                d[3],

            ])
        file_name = 'bank_form_csv' + "_YESbank-"+ '.csv'
        # file_from = '/tmp/' + file_name
        file_from = file_name
        with open(file_from, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)
        file_to = '/buymore2/employees/Bankform/' + file_name
        access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
        dbx = dropbox.Dropbox(access_token)

        with open(file_from, 'rb') as f:
            dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)
        ###for csv file download link
        # file_name = "PaySlip-EMP_ID-" + emp_id + month1 + ".pdf"
        print("file_name===", file_name)
        file_path = '/buymore2/employees/Bankform/' + file_name
        link = dbx.files_get_temporary_link(file_path).link
    else:
        engine = create_engine(cred_for_sqlalchemy_emp)
        query1 = "SELECT api_employee.bank_acc_number,api_employee.name,api_employee.ifsc_code,api_monthlyempsalary.net_employee_payable " \
                 "FROM api_employee, api_monthlyempsalary " \
                 "where api_employee.ifsc_code NOT LIKE 'YES%%' and api_employee.emp_id = api_monthlyempsalary.emp_id_id and status = 2 and month = '" + str(
            month) + "'"
        print(query1)
        sql = pd.read_sql(query1, engine)
        sql.to_csv('a2.csv',index=False)

        cc4455_final1 = pd.read_csv("a2.csv")
        cc4455_final1['Txn Type']='NEFT'
        cc4455_final1.to_csv('a4.csv',index=False)


        engine.dispose()
        data = [[
            'Txn Type',
            'Credit Ac Number',
            'Credit Account Name',
            'IFSC',
            'Amount',
        ]]

        for index, d in cc4455_final1.iterrows():
            # for index, d in cc1.iterrows():
            print("d", d)
            data.append([
                d[4],
                d[0],
                d[1],
                d[2],
                d[3],



            ])
        file_name = 'bank_form_csv' + "_Other_Bank"+ '.csv'
        # file_from = '/tmp/' + file_name
        file_from = file_name
        with open(file_from, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)
        file_to = '/buymore2/employees/Bankform/' + file_name
        access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'
        dbx = dropbox.Dropbox(access_token)

        with open(file_from, 'rb') as f:
            dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)
        ###for csv file download link
        # file_name = "PaySlip-EMP_ID-" + emp_id + month1 + ".pdf"
        print("file_name===", file_name)
        file_path = '/buymore2/employees/Bankform/' + file_name
        link = dbx.files_get_temporary_link(file_path).link
    return {
        'status': True,
        'link': link
    }

print(lambda_handler())
