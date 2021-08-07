import pymongo as pym      # Interface with Python <--> MongoDB
import pandas as pd        # Create Dataframe
import os                  # find files on system
import csv                 # to dump as a csv file
import json                # convert file to json format
from bson.code import Code  # helps with map-reduce
from pymongo import MongoClient
import pandas as pd
# Making a Connection to MongoClient
client = MongoClient(
    'mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/wms?retryWrites=true&w=majority')
db = client.wms
# client = pym.MongoClient('mongodb://localhost:27017/')

# DATABASE connection:
# db = client["berkeley"]

# CREATING A COLLECTION (*AKA* TABLE):
recruiter_candidates= db["api_reportcenter"]

###
abbrev_ppl=list(recruiter_candidates.find({},{'_id':0,'vendor_id':1,'report_type':1,'report_input':1,'is_completed':1,'file_path':1,'generated_by':1}))

abbrev_ppl[:2]

# import csv

fields = ['vendor_id','report_type','report_input','is_completed','file_path','generated_by']
with open('candidate_ppl.csv','w') as outfile:
    write=csv.DictWriter(outfile, fieldnames=fields)
    write.writeheader()
    for x in abbrev_ppl:
        print(x)
        write.writerow(x)
        ##for nested data use this nested looping
        # for y, v in x.items():
        #     print("print---y",y)
        #     print("print--v", v)
        #     if y == 'candidate':
        #         print (y, v)
        #         write.writerow(v)

data=pd.read_csv('candidate_ppl.csv')
data=data.drop(axis=1,columns=['file_path','generated_by','report_input'])
data.to_csv('a1.csv',index=False)