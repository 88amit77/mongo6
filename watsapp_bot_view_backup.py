# class watsappbot(APIView):
#     def post(self, request):
#         import requests
#         import json
#         def send_wahts_msg(sender_number,text_to_be_sent):
#             url = "https://panel.rapiwha.com/send_message.php"
#
#             querystring = {"apikey": "ULH7UY29UJLQO4WF61FE", "number": sender_number, "text": text_to_be_sent}
#
#             response = requests.request("GET", url, params=querystring)
#
#             return response.text
#         def check_order_presence(oid):
#             import psycopg2
#             rds_host = "buymore2.cegnfd8ehfoc.ap-south-1.rds.amazonaws.com"
#             name = "postgres"
#             password = "buymore2"
#             db_name = "orders"
#             conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
#             cur = conn.cursor()
#             qry = "SELECT no.order_id,no.product_id,dd.name from api_neworder no,api_dispatchdetails dd WHERE no.dd_id=dd.dd_id_id AND order_id='"+str(oid)+"'"
#             print(qry)
#             cur.execute(qry)
#             orders = cur.fetchall()
#             cur.close()
#             conn.close()
#             if len(orders)>0:
#                 return orders[0]
#             else:
#                 return []
#         def get_whats_msg(qr):
#             url1="http://panel.rapiwha.com/get_messages.php"
#             response = requests.request("GET", url1, params=qr)
#             result=response.text[1:-1].split("}")
#             resp=[]
#             for r in result:
#                 if r.startswith("{"):
#                 # r+="}"
#                 # print(json.loads(r))
#                     resp.append(json.loads(r+"}"))
#                 elif r.startswith(",{"):
#                 # r=r[1:]+"}"
#                 # print(json.loads(r))
#                     resp.append(json.loads(r[1:]+"}"))
#                 else:
#                     pass
#             return resp
#         # qr={"apikey":"P68S66E6LZISKNF6VEXC","type":"IN","markaspulled":"1","getnotpulledonly":"1"}
#         # recieved_msgs=get_whats_msg(qr)
#         recieved_msgs=request.data
#         ev=recieved_msgs["event"]
#         if ev=='INBOX':
#             if True:
#                 sender = recieved_msgs['from']
#                 msg_type = ev
#                 recieved_msg = recieved_msgs['text']
#                 qr = {"apikey": "ULH7UY29UJLQO4WF61FE", "number": sender, "type": "OUT", "markaspulled": "1",
#                       "getnotpulledonly": "1"}
#                 rvmsg = get_whats_msg(qr)[-1:]
#                 sent_msg = ''
#                 if len(rvmsg) > 0:
#                     sent_msg = rvmsg[0]['text']
#                 if msg_type=='INBOX':
#                     check_box=['How was your shopping experience. 1. Very good 2. It was average 3. Not happy - want to return the product Please type any number to allow us to know your experience. Eg : type 1 if you were very happy with your shopping experience','can you please give us your order id as mentioned in your invoice',]
#                     if recieved_msg=='Hi !! I just purchased one of your products.' and (sent_msg=='' or sent_msg not in check_box):
#                         rpl_msg="it’s an honour , that you preferred Buymore as your choice of seller for purchasing the product. There are so many options out there but you singled us out and we really appreciate your business."
#                         print('sending ' + sender + ' an automated msg - ' + rpl_msg)
#                         print('response is a below')
#                         print(send_wahts_msg(sender, rpl_msg))
#                         rpl_msg="can you please give us your order id as mentioned in your invoice"
#                         print('sending ' + sender + ' an automated msg - ' + rpl_msg)
#                         print('response is a below')
#                         print(send_wahts_msg(sender, rpl_msg))
#                     else:
#                         if len(rvmsg)>0:
#                             if sent_msg in ['How was your shopping experience. 1. Very good 2. It was average 3. Not happy - want to return the product Please type any number to allow us to know your experience. Eg : type 1 if you were very happy with your shopping experience']:
#                                 if recieved_msg in ["1","2","3"]:
#                                     if recieved_msg=="1":
#                                         rpl_msg='we are greatful that you liked our product and the service'
#                                     elif recieved_msg=="2":
#                                         rpl_msg='Please let us know what went wrong so that we can ensure better services next time around'
#                                     else:
#                                         rpl_msg='Kindly elloborate on your issue so that we can arrange a call back from our representative to initiate pickup of our product as soon as possible'
#                                     print('sending ' + sender + ' an automated msg - ' + rpl_msg)
#                                     print('response is a below')
#                                     print(send_wahts_msg(sender, rpl_msg))
#                                 else:
#                                     rpl_msg='OOPS,we think you have entered a wrong value'
#                                     print('sending ' + sender + ' an automated msg - ' + rpl_msg)
#                                     print('response is a below')
#                                     print(send_wahts_msg(sender, rpl_msg))
#                                     rpl_msg='How was your shopping experience. 1. Very good 2. It was average 3. Not happy - want to return the product Please type any number to allow us to know your experience. Eg : type 1 if you were very happy with your shopping experience'
#                                     print('sending ' + sender + ' an automated msg - ' + rpl_msg)
#                                     print('response is a below')
#                                     print(send_wahts_msg(sender, rpl_msg))
#                             elif sent_msg in ['can you please give us your order id as mentioned in your invoice']:
#                                 ord_det=check_order_presence(recieved_msg)
#                                 if ord_det!=[]:
#                                     rpl_msg=str(ord_det[2])
#                                     if rpl_msg!='N/A' and rpl_msg!='None':
#                                         print('sending ' + sender + ' an automated msg - ' + rpl_msg)
#                                         print('response is a below')
#                                         print(send_wahts_msg(sender, "Hi, "+rpl_msg))
#                                     rpl_msg='How was your shopping experience. 1. Very good 2. It was average 3. Not happy - want to return the product Please type any number to allow us to know your experience. Eg : type 1 if you were very happy with your shopping experience'
#                                     print('sending ' + sender + ' an automated msg - ' + rpl_msg)
#                                     print('response is a below')
#                                     print(send_wahts_msg(sender, rpl_msg))
#                                 else:
#                                     rpl_msg = "sorry we couldnot find the order id you entered"
#                                     print('sending ' + sender + ' an automated msg - ' + rpl_msg)
#                                     print('response is a below')
#                                     print(send_wahts_msg(sender, rpl_msg))
#                                     rpl_msg = "can you please give us your order id as mentioned in your invoice"
#                                     print('sending ' + sender + ' an automated msg - ' + rpl_msg)
#                                     print('response is a below')
#                                     print(send_wahts_msg(sender, rpl_msg))
#                 else:
#                     print(recieved_msgs)
#         else:
#             print('no new messages')
#         return HttpResponse('sucess')