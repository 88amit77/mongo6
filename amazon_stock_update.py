import json, math, time, csv, logging
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
from datetime import datetime, timedelta
import requests, dropbox, mws, psycopg2
from throttle import throttle
from pymongo import MongoClient


def db_credential(db_name, typ):
    # import requests
    # import json
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
    print(response, type(response))
    if status == True:
        return response['db_detail'][typ]
    else:
        return


db_creds = db_credential('postgres', 'db_detail_for_psycopg2')
# db_creds={"endPoint":"buymore-dev-aurora.cluster-cegnfd8ehfoc.ap-south-1.rds.amazonaws.com","userName":"postgres","passWord":"r2DfZEyyNL2VLfg2"}

RDS_HOST = db_creds['endPoint']
NAME = db_creds['userName']
PASSWORD = db_creds['passWord']
DB_NAME = "products"
AMAZON_PORTAL_ID = 1
access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavph'


def generateXmlFile(dicList):
    """
    """

    today = datetime.now()
    lastTimedelta = today + timedelta(days=-1)
    lastDate = lastTimedelta.isoformat()
    nextTimedelta = today + timedelta(days=365)

    nextYearDate = nextTimedelta.isoformat()
    xml = Element('AmazonEnvelope')
    xml.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
    xml.set('xsi:noNamespaceSchemaLocation', 'amzn-envelope.xsd')
    header = SubElement(xml, 'Header')
    doc = SubElement(header, 'DocumentVersion')
    doc.text = "1.01"
    merch = SubElement(header, 'MerchantIdentifier')
    merch.text = "A1JMG6531Z3EIJ"
    msgtype = SubElement(xml, 'MessageType')
    msgtype.text = "Price"

    for count, item in enumerate(dicList, 1):
        """ """
        message = SubElement(xml, 'Message')
        messageId = SubElement(message, 'MessageID')
        messageId.text = str(count)
        price_value = SubElement(message, 'Price')
        sku_value = SubElement(price_value, 'SKU')
        sku_value.text = item['amazon_portal_sku']

        standard_price = SubElement(price_value, 'StandardPrice')
        standard_price.text = str(item['product_mrp'])
        standard_price.set('currency', 'INR')

        selling_price_xml = SubElement(price_value, 'Sale')
        oneDayBefore = SubElement(selling_price_xml, 'StartDate')
        oneDayBefore.text = lastDate
        afterOneWeek = SubElement(selling_price_xml, 'EndDate')
        afterOneWeek.text = nextYearDate
        sale_price = SubElement(selling_price_xml, 'SalePrice')
        sale_price.text = str(math.floor(item['amazon_upload_selling_price']))
        sale_price.set('currency', 'INR')

    return xml


@throttle(minutes=2)
def processing_request(singleRequest, feed, logger, products_cursor, conn_products, writer):
    """
    Processing MWS Feed Requeste and handling throttle
    """
    try:
        product_id_str_list = ",".join(list(map(lambda x: str(x['product_id']), singleRequest)))

        logger.info("product_id_str_list------{}".format(product_id_str_list))

        writer.writerow({'API Process Status': "product_id_str_list", 'Response': product_id_str_list})

        xmlData = generateXmlFile(singleRequest)
        feed_response = feed.submit_feed(tostring(xmlData), "_POST_PRODUCT_PRICING_DATA_", 'A21TJRUUN4KGV')
        logger.info("XML Data of processing-----{}".format(tostring(xmlData)))
        writer.writerow({'API Process Status': "XML Data of processing", 'Response': tostring(xmlData)})
        feed_response_data = feed_response.parsed

        logger.info("feed_response_data------{}".format(feed_response_data))
        writer.writerow({'API Process Status': "feed_response_data", 'Response': feed_response_data})
        feedSubmissionInfo = feed_response_data.get('FeedSubmissionInfo', None)
        logger.info("feedSubmissionInfo------{}".format(feedSubmissionInfo))
        if feedSubmissionInfo is not None and isinstance(feedSubmissionInfo, dict):
            processing_status = feedSubmissionInfo.get('FeedProcessingStatus', {}).get('value', None)
            logger.info("processing_status------{}".format(processing_status))
            if processing_status is not None and processing_status == "_SUBMITTED_":
                feedSubmissionId = feedSubmissionInfo.get('FeedSubmissionId', {}).get('value', None)
                logger.info("feedSubmissionId------{}".format(feedSubmissionId))
                if feedSubmissionId is not None:
                    api_status = 0
                    while api_status == 0:
                        time.sleep(120)  # Wait 1 min for new request
                        response = feed.get_feed_submission_list([feedSubmissionId])
                        response_parsed_data = response.parsed
                        logger.info("response_parsed_data------{}".format(response_parsed_data))
                        writer.writerow(
                            {'API Process Status': "response_parsed_data", 'Response': response_parsed_data})
                        feed_sub_status = response_parsed_data.get('FeedSubmissionInfo', {}).get('FeedProcessingStatus',
                                                                                                 {}).get('value', None)
                        logger.info("feed_sub_status------{}".format(feed_sub_status))
                        if feed_sub_status in ['_AWAITING_ASYNCHRONOUS_REPLY_', '_SUBMITTED_', '_IN_PROGRESS_',
                                               '_UNCONFIRMED_', None]:
                            api_status = 0
                            logger.info("Waiting for requests ------{}".format(feed_sub_status))
                        elif feed_sub_status in ['_CANCELLED_', '_DONE_NO_DATA_', '_IN_SAFETY_NET_']:
                            api_status = 1
                            logger.info("Request terminating after ------{}".format(feed_sub_status))
                        elif feed_sub_status == "_DONE_":

                            submission_result = feed.get_feed_submission_result(feedSubmissionId)
                            submission_result_data = submission_result.parsed
                            logger.info("submission_result_data------{}".format(submission_result_data))
                            processingSummary = submission_result_data.get('ProcessingReport', {}).get(
                                'ProcessingSummary', None)
                            if processingSummary is not None:
                                messagesProcessed = processingSummary.get('MessagesProcessed', {}).get('value', -99999)
                                messagesSuccessful = processingSummary.get('MessagesSuccessful', {}).get('value',
                                                                                                         -99999)
                                messagesWithError = processingSummary.get('MessagesWithError', {}).get('value', -99999)
                                messagesWithWarning = processingSummary.get('MessagesWithWarning', {}).get('value',
                                                                                                           -99999)
                                api_status = 1
                                logger.info("messagesProcessed------{}".format(messagesProcessed))
                                logger.info("messagesSuccessful------{}".format(messagesSuccessful))
                                logger.info("messagesWithError------{}".format(messagesWithError))
                                logger.info("messagesWithWarning------{}".format(messagesWithWarning))
                                writer.writerow(
                                    {'API Process Status': "messagesProcessed", 'Response': messagesProcessed})
                                writer.writerow(
                                    {'API Process Status': "messagesSuccessful", 'Response': messagesSuccessful})
                                writer.writerow(
                                    {'API Process Status': "messagesWithError", 'Response': messagesWithError})
                                writer.writerow(
                                    {'API Process Status': "messagesWithWarning", 'Response': messagesWithWarning})
                                products_cursor.execute(
                                    "UPDATE amazon_amazonproducts SET amazon_current_selling_price=amazon_upload_selling_price  WHERE product_id in (" + product_id_str_list + ")")
                                conn_products.commit()
                                product_id_str_list = ""

    except Exception as e:
        message = "Processing Exception--" + str(e)
        logger.info("Processing Exception------{}".format(message))

    finally:
        pass


def lambda_handler(event, context):
    """
    """
    try:
        total_success = total_fail = 0

        conn_products = psycopg2.connect(database="products", host=RDS_HOST, user=NAME, password=PASSWORD)

        ## FOR Mongodb Request
        client = MongoClient(
            'mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/wms?retryWrites=true&w=majority')
        db = client.wms
        price_update_table = db.portal_price_update_logs

        file_name = "{0}.csv".format(datetime.now().strftime("%Y%m%d%H%M%S"))
        file_path = '/tmp/' + file_name
        file_obj = open(file_path, "w+")
        fieldnamesList = ['API Process Status', 'Response']

        writer = csv.DictWriter(file_obj, fieldnames=fieldnamesList)
        writer.writeheader()

        products_cursor = conn_products.cursor()
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        statusCode = 200
        message = "Requested"

        access_token_list = {}
        product_id_list = event.get("product_id_list", None)

        if product_id_list is not None and len(product_id_list) != 0:
            product_id_str = ",".join(product_id_list)
            products_cursor.execute(
                "SELECT mp.product_id,ap.amazon_account_id,ap.amazon_portal_sku,ap.amazon_unique_id,ap.amazon_listing_id,ap.amazon_current_selling_price,ap.amazon_upload_selling_price,mp.product_mrp,ap.amazon_min_break_even_sp,ap.amazon_max_break_even_sp,ap.amazon_price_rule,ap.amazon_competitor_lowest_price,mp.product_length,mp.product_breath,mp.product_width,mp.product_weight,mp.min_payout_value,mp.max_payout_value,mp.hsn_code_id_id FROM amazon_amazonproducts as ap left join master_masterproduct as mp on ap.product_id = mp.product_id where ap.product_id in (" + product_id_str + ")")
        else:
            products_cursor.execute(
                "SELECT mp.product_id,ap.amazon_account_id,ap.amazon_portal_sku,ap.amazon_unique_id,ap.amazon_listing_id,ap.amazon_current_selling_price,ap.amazon_upload_selling_price,mp.product_mrp,ap.amazon_min_break_even_sp,ap.amazon_max_break_even_sp,ap.amazon_price_rule,ap.amazon_competitor_lowest_price,mp.product_length,mp.product_breath,mp.product_width,mp.product_weight,mp.min_payout_value,mp.max_payout_value,mp.hsn_code_id_id FROM amazon_amazonproducts as ap left join master_masterproduct as mp on ap.product_id = mp.product_id where ap.amazon_current_selling_price != ap.amazon_upload_selling_price ORDER BY ap.amazon_account_id DESC LIMIT 10000")

        amazon_response = products_cursor.fetchall()
        products_column_names = [desc[0] for desc in products_cursor.description]
        amazon_results_data = {}
        access_token_list = {}
        if len(amazon_response) != 0:
            products_cursor.execute(
                "Select account_id,authentication_attribute_values from master_portalaccountdetails where portal_id_id = " + str(
                    AMAZON_PORTAL_ID))
            portal_accounts = products_cursor.fetchall()
            if len(portal_accounts):
                for portal_account in portal_accounts:
                    for item in portal_account[1]:
                        if portal_account[0] not in access_token_list:
                            access_token_list[portal_account[0]] = {}

                        access_token_list[portal_account[0]][item['name']] = item['base_url']

            for single_record in amazon_response:
                if int(single_record[1]) not in amazon_results_data:
                    amazon_results_data[int(single_record[1])] = []

                # Create dic list based on account id
                amazon_results_data[int(single_record[1])].append(dict(zip(products_column_names, single_record)))

        list_length = 10000

        if len(amazon_results_data) != 0:
            print("amazon_results_data====", amazon_results_data)

            for account_id in amazon_results_data:
                print("account_id--->", account_id)
                print("account_id--->", type(account_id))
                print(access_token_list)
                account = access_token_list.get(account_id, None)
                products_data = amazon_results_data.get(account_id)
                print("account", account)
                print("len of products-->", len(products_data))
                if account is not None and len(products_data) != 0:
                    print("access_token_list-----", account)
                    feed = mws.Feeds(access_key=account['access_key'], secret_key=account['secret_key'],
                                     account_id=account['seller_id'], region='IN')
                    amazon_results_chunk = [products_data[i:i + list_length] for i in
                                            range(0, len(products_data), list_length)]

                    logger.info("Requested No of Products-----{}".format(len(products_data)))
                    writer.writerow({'API Process Status': "Requested No of Products", 'Response': len(products_data)})

                    for singleRequest in amazon_results_chunk:
                        """ Calling func for processing_request """
                        print(singleRequest)

                        processing_request(singleRequest, feed, logger, products_cursor, conn_products, writer)

                        for singleitem in singleRequest:
                            mongodb_insert = {
                                "portal_id": AMAZON_PORTAL_ID,
                                "product_id": singleitem['product_id'],
                                "portal_unique_id": singleitem['amazon_unique_id'],
                                "portal_listing_id": singleitem['amazon_listing_id'],
                                "min_selling_price": singleitem['amazon_min_break_even_sp'],
                                "max_selling_price": singleitem['amazon_max_break_even_sp'],
                                "price_rule": singleitem['amazon_price_rule'],
                                "product_length": singleitem['product_length'],
                                "product_breath": singleitem['product_breath'],
                                "product_width": singleitem['product_width'],
                                "product_weight": singleitem['product_weight'],
                                "current_selling_price": singleitem['amazon_current_selling_price'],
                                "upload_selling_price": singleitem['amazon_upload_selling_price'],
                                "competitor_lowest_price": singleitem['amazon_competitor_lowest_price'],
                                "min_payout_value": singleitem['min_payout_value'],
                                "max_payout_value": singleitem['max_payout_value'],
                                "account_id": singleitem['amazon_account_id'],
                                "hsn_code_id": singleitem['hsn_code_id_id'],
                                "created_at": datetime.fromisoformat(str(datetime.now()))
                            }
                            price_update_table.insert_one(mongodb_insert)

    except Exception as e:
        statusCode = 400
        message = "Exception--" + str(e)
        logger.info("product_id-----{}".format(str(e)))
        writer.writerow({'API Process Status': "Exception", 'Response': str(e)})
    finally:
        conn_products.close()
        products_cursor.close()
        file_obj.close()

        file_to = '/buymore2/price_update/amazon/' + file_name
        dbx = dropbox.Dropbox(access_token)

        with open(file_path, 'rb') as f:
            data = dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)

        return {'statusCode': 200, 'message': message, "total_success": total_success, "total_fail": total_fail, }

