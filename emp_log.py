####10
import json
from pymongo import MongoClient
import csv
from datetime import datetime
import requests
import dropbox
import psycopg2
import sys
import re
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://postgres:buymore2@buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com/orders')
order_item_id='11818200514371100'
query = "SELECT dd_id from api_neworder where order_item_id = '11818200514371100'"
master_stock_df = pd.read_sql(query, engine)
print("master_stock_df==",master_stock_df)
dd_id=master_stock_df.at[0,'dd_id']
print("dd_id==",dd_id)
awb="123"
invoice="321"
update_query="update api_dispatchdetails set awb = "+str(awb)+",invoice = "+str(invoice)+" where dd_id_id = "+str(dd_id)
print(update_query)

engine.dispose()