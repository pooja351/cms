import json
from datetime import datetime, timedelta
import base64
from db_connection import db_connection_manage
import sys
import os
from cms_utils import file_uploads
import re

conct = db_connection_manage()
def file_get(path):
    if path:
        return path
    else:
        return ""
class PurchaseReturn():
    def cmsPurchaseReturnGetOrderId(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            #print(data)
            # databaseTableName = "PtgCms"+data["env_type"]
            # gsipk_table="PurchaseOrder"
            # dynamodb = boto3.client('dynamodb')
            # statement = f"""select pk_id from {databaseTableName} where  gsipk_table = '{gsipk_table}'"""
            # qaData = execute_statement_with_pagination(statement)
            # sorted_pk_ids1 = extract_items_from_array_of_nested_dict(qaData)
            sorted_pk_ids1 = list(db_con.PurchaseOrder.find({}))
            if sorted_pk_ids1:
                sorted_pk_ids = sorted([item["pk_id"] for item in sorted_pk_ids1], reverse=True)
                sorted_pk_ids2 = sorted(sorted_pk_ids, key=lambda x: int(x.replace("OPTG", "")), reverse=False)
                conct.close_connection(client)
                return {"statusCode": 200, "body": sorted_pk_ids2}
            else:
                conct.close_connection(client)
                return {"statusCode": 404, "body": "No Data"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

    def cmsPurchaseReturnGetInwardId(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            inwardId = data['po_order_id']
            qaData = list(db_con.QATest.find({"all_attributes.po_id": inwardId},
                                             {"all_attributes.inwardId": 1, "all_attributes.parts": 1}))
            if qaData:
                a = [item['all_attributes']['inwardId'] for item in qaData if
                     any(int(k['fail_qty']) > 0 for j, k in item['all_attributes']['parts'].items())]
                #print(a)
                sorted_pk_ids2 = sorted(a, key=lambda x: int(x.split('_')[1][2:]))
                conct.close_connection(client)
                return {"statusCode": 200, "body": sorted_pk_ids2}
            else:
                conct.close_connection(client)
                return {"statusCode": 404, "body": "No Data"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

    def cmsGetComponentDetailsInsidePurchaseReturnModified(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            #print(data)
            # databaseTableName = "PtgCms"+data["env_type"]             # gsipk_table="QATest"
            po_id = data['po_order_id']
            inwardId = data['inwardId']
            # dynamodb = boto3.client('dynamodb')
            # statement = f"""select * from {databaseTableName} where  gsipk_table = '{gsipk_table}' and all_attributes.inwardId='{inwardId}' and all_attributes.po_id='{po_id}' """
            # qaData = execute_statement_with_pagination(statement)
            sorted_pk_ids_list = list(
                db_con.QATest.find({"all_attributes.inwardId": inwardId, "all_attributes.po_id": po_id}))
            # sorted_pk_ids_list = [sorting_function(item) for item in qaData]
            # category_statement = f"""select gsisk_id,sub_categories,pk_id from {databaseTableName} where gsipk_table='Metadata' and gsipk_id='Electronic' """
            # category_data = execute_statement_with_pagination(category_statement)
            category_data = list(
                db_con.Metadata.find({"gsipk_id": "Electronic"}, {"gsisk_id": 1, "sub_categories": 1, "pk_id": 1}))
            category_data = {item['pk_id'].replace("MDID", "CTID"): {"ctgr_name": item['gsisk_id'],
                                                                     "sub_categories": item['sub_categories']} for item
                             in category_data}
            # inventory = f"select all_attributes.description,all_attributes.package,all_attributes.manufacturer,all_attributes.cmpt_id,all_attributes.prdt_name,all_attributes.sub_ctgr from {databaseTableName} where gsipk_table='Inventory'"
            # inventory = extract_items_from_array_of_nested_dict(execute_statement_with_pagination(inventory))
            inventory = list(db_con.Inventory.find({}, {"all_attributes.description": 1, "all_attributes.package": 1,
                                                        "all_attributes.manufacturer": 1, "all_attributes.cmpt_id": 1,
                                                        "all_attributes.prdt_name": 1, "all_attributes.sub_ctgr": 1}))
            inventory = {item['all_attributes']['cmpt_id']: item['all_attributes'] for item in inventory}
            # return category_data
            b = []
            if sorted_pk_ids_list:
                # return sorted_pk_ids_list
                filtered_list = [
                    {
                        "qa_date": item['all_attributes']["QA_date"] if "QA_date" in item['all_attributes'] else " ",
                        "sender_name": item['all_attributes']["sender_name"] if "sender_name" in item[
                            'all_attributes'] else "",
                        "sender_contact_number": item['all_attributes'][
                            "sender_contact_number"] if "sender_contact_number" in item['all_attributes'] else "",
                        # "invoice": file_get(item['all_attributes']["invoice"]) if "invoice" in item['all_attributes'] else "",
                        # "qa_test": file_get(item['all_attributes']["QATest"]) if "QATest" in item['all_attributes'] else "",
                        "invoice": item["all_attributes"]["invoice"] if "invoice" in item["all_attributes"] else "",
                        "qa_test": item["all_attributes"]["QATest"] if "QATest" in item["all_attributes"] else "",
                        # "photo": item['all_attributes']["photo"] if "photo" in item['all_attributes'] else "",

                        # "photo": {k:file_get(v)  for k, v in item['all_attributes']["photo"].items()} if "photo" in item['all_attributes'] else "",
                        "photo": item["all_attributes"].get("photo", {}),


                        "description": item['all_attributes']["description"] if "description" in item[
                            'all_attributes'] else "",
                        "parts": {
                            part_key: {
                                "cmpt_id": part_data['cmpt_id'],
                                "ctgr_id": part_data['ctgr_id'],
                                "price_per_piece": part_data['price_per_piece'],
                                "description": part_data['description'],
                                "packaging": part_data['packaging'],
                                "fail_qty": part_data['fail_qty'],
                                "batchId": part_data['batchId'],
                                "mfr_prt_num": part_data['mfr_prt_num'],
                                "manufacturer": part_data['manufacturer'],
                                "price": int(float(part_data['price_per_piece']) * float(part_data['fail_qty'])),
                                "pass_qty": part_data['pass_qty'],
                                "qty": part_data['qty'],
                                "department": part_data['department'],
                                "prdt_name": category_data[part_data['ctgr_id']]['sub_categories'][
                                    inventory[part_data['cmpt_id']]['sub_ctgr']] if part_data[
                                                                                        'department'] == 'Electronic' else
                                part_data['prdt_name']
                            }
                            for part_key, part_data in item['all_attributes']['parts'].items()
                            if int(part_data['fail_qty']) > 0

                        }
                    }
                    for item in sorted_pk_ids_list
                ]
                if filtered_list:
                    conct.close_connection(client)
                    return {"statusCode": 200, "body": filtered_list}
                else:
                    conct.close_connection(client)
                    return {"statusCode": 200, "body": "we cant return fail qty is zero"}

            else:
                conct.close_connection(client)
                return {"statusCode": 404, "body": "No Data"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

    def CmsPurchaseReturnCreateModified(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            all_attributes = {}
            po_id = data['order_id']
            gsipk_table = "PurchaseReturn"
            description = data['description']
            inward_id = data['inward_id']
            result1 = list(db_con.PurchaseOrder.find({"pk_id": po_id}))
            result3 = list(db_con.QATest.find({"all_attributes.inwardId": inward_id}))
            parts = result3[0]['all_attributes']['parts']
            matching_parts = {f"part{index + 1}": value for index, part in enumerate(data['parts']) for key, value in
                              parts.items() if value['mfr_prt_num'] == part['mfr_prt_num']}
            result = list(db_con.PurchaseReturn.find({"gsisk_id": po_id}))
            return_id = f"{data['order_id']}-R1"
            if result:
                if any(item['gsipk_id'] == data['inward_id'] for item in result):
                    return {'statusCode': 400, 'body': "Return already present for given inward id"}
                returnids = sorted([int(item["pk_id"].split("R")[-1]) for item in result], reverse=True)
                return_id = f"{data['order_id']}-R{returnids[0] + 1}"

            sk_timeStamp = (datetime.now()).isoformat()
            parts = {f"part{inx + 1}": value for inx, value in enumerate(data['parts'])}
            if result1:
                vendor_id = [i['all_attributes']['vendor_id'] for i in result1]
                #print(vendor_id)
                result2 = list(db_con.Vendor.find({"pk_id": vendor_id[0]}))
                #print(result2)
                if result2:
                    if parts:
                        all_attributes = {}
                        all_attributes['return_id'] = return_id
                        all_attributes['parts'] = parts
                        all_attributes['invoice'] = [i['all_attributes']['invoice'] for i in result3][0]
                        all_attributes['qa_test'] = [i['all_attributes']['QATest'] for i in result3][0]
                        all_attributes['photo'] = {i: j for i, j in result3[0]['all_attributes']['photo'].items()}
                        all_attributes['inward_id'] = inward_id
                        all_attributes['qa_date'] = [i['all_attributes']['QA_date'] for i in result3][0]
                        all_attributes['sender_name'] = [i['all_attributes']['sender_name'] for i in result3][0]
                        all_attributes['sender_contact_number'] = \
                        [i['all_attributes']['sender_contact_number'] for i in result3][0]
                        all_attributes['status'] = data['status']
                        all_attributes['description'] = description
                        all_attributes['vendor_id'] = [i['all_attributes']['vendor_id'] for i in result2][0]
                        all_attributes['vendor_name'] = [i['all_attributes']['vendor_name'] for i in result2][0]
                        all_attributes['bank_name'] = [i['all_attributes']['bank_name'] for i in result2][0]
                        all_attributes['account_number'] = [i['all_attributes']['account_number'] for i in result2][0]
                        all_attributes['gst_number'] = [i['all_attributes']['gst_number'] for i in result2][0]
                        all_attributes['ifsc_code'] = [i['all_attributes']['ifsc_code'] for i in result2][0]
                        all_attributes['address1'] = [i['all_attributes']['address1'] for i in result2][0]
                        all_attributes['email'] = [i['all_attributes']['email'] for i in result2][0]
                        all_attributes['contact_number'] = [i['all_attributes']['contact_number'] for i in result2][0]
                        all_attributes['order_date'] = [i['sk_timeStamp'] for i in result1][:10][0]
                        all_attributes['total_price'] = [i['all_attributes']['total_price'] for i in result1][0]
                        # return all_attributes
                        # all_attributes=create_nested_dict(all_attributes)
                        item = {
                            "pk_id": return_id,
                            "sk_timeStamp": sk_timeStamp,
                            "all_attributes": all_attributes,
                            "gsipk_table": gsipk_table,
                            "gsisk_id": data['order_id'],
                            "gsipk_id": data['inward_id'],
                            "lsi_key": "--"
                        }
                        updat = db_con.PurchaseReturn.insert_one(item)
                        for i, j in matching_parts.items():
                            cmpt_id = j['cmpt_id']
                            result4 = list(db_con.Inventory.find({"pk_id": cmpt_id}, {"all_attributes.rtn_qty": 1,
                                                                                      "all_attributes.cmpt_id": 1,
                                                                                      "pk_id": 1, "sk_timeStamp": 1}))
                            if result4:
                                inventory_item = result4[0]
                                if 'rtn_qty' in inventory_item:
                                    existing_qty = int(inventory_item['all_attributes']['rtn_qty'])
                                else:
                                    existing_qty = 0
                                new_fail_qty = int(j['fail_qty'])
                                total_qty = existing_qty + new_fail_qty
                                resp = db_con.Inventory.update_one(
                                    {"pk_id": inventory_item['pk_id']},
                                    {"$set": {"all_attributes.rtn_qty": str(total_qty)}}
                                )
                        try:
                            response = {
                                'statusCode': 200,
                                'body': 'Purchase Return added successfully'
                            }
                            return response

                        except Exception as err:
                            exc_type, exc_obj, tb = sys.exc_info()
                            f_name = tb.tb_frame.f_code.co_filename
                            line_no = tb.tb_lineno
                            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
                            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
                    else:
                        return {'statusCode': 204, 'body': "parts are not added please check"}
                else:
                    return {'statusCode': 404, 'body': "vendors data is not there please check"}
            else:
                return {'statusCode': 404, 'body': "po data is not there"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

    def cmsPurchaseReturnGetInternalDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            return_id = data['return_id']
            result = list(db_con.PurchaseReturn.find({"all_attributes.return_id": return_id}))
            result1 = result[0]
            if result1:

                lst = {
                    "inward_id": result1["all_attributes"]["inward_id"],
                    "invoice": result1["all_attributes"]["invoice"],
                    "parts": result1["all_attributes"]["parts"],
                    "photo": result1["all_attributes"]["photo"],
                    "qa_date": result1["all_attributes"]["qa_date"],
                    "qa_test": result1["all_attributes"]["qa_test"],
                    "return_id": result1["all_attributes"]["return_id"],
                    "sender_contact_number": result1["all_attributes"]["sender_contact_number"],
                    "sender_name": result1["all_attributes"]["sender_name"],
                    "vendor_id": result1["all_attributes"]["vendor_id"],
                    "vendor_name": result1["all_attributes"]["vendor_name"],
                    "address1": result1["all_attributes"]["address1"],
                    "email": result1["all_attributes"]["email"],
                    "bank_name": result1["all_attributes"]["bank_name"],
                    "account_number": result1["all_attributes"]["account_number"],
                    "gst_number": result1["all_attributes"]["gst_number"],
                    "ifsc_code": result1["all_attributes"]["ifsc_code"],
                    "contact_number": result1["all_attributes"]["contact_number"],
                    "description": result1['all_attributes']["description"],
                    "return_date": result1['sk_timeStamp'][:10],
                    "order_date": result1['all_attributes']["order_date"][:10],
                    "received_date": result1['all_attributes']["qa_date"],
                    "order_id": result1['gsisk_id']
                }
                conct.close_connection(client)
                return {'statusCode': 200, 'body': lst}
            else:
                conct.close_connection(client)
                return {'statusCode': 200, 'body': []}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

    def cmsPurchaseReturnEditGetDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            return_id = data['return_id']
            result = list(db_con.PurchaseReturn.find({"all_attributes.return_id": return_id},
                                                     {"all_attributes": 1, "sk_timeStamp": 1}))
            result1 = result[0]
            if result1:
                lst = [{
                    "inward_id": result1["all_attributes"]["inward_id"],
                    "invoice": file_get(result1["all_attributes"]["invoice"]),
                    "parts": result1["all_attributes"]["parts"],
                    # "photo": result1["all_attributes"]["photo"],
                    "photo": {k: file_get(v) for k, v in result1['all_attributes']["photo"].items()},

                    # "photo": result1["all_attributes"]["photo"],
                    "qa_date": result1["all_attributes"]["qa_date"],
                    "qa_test": file_get(result1["all_attributes"]["qa_test"]),
                    "return_id": result1["all_attributes"]["return_id"],
                    "sender_contact_number": result1["all_attributes"]["sender_contact_number"],
                    "sender_name": result1["all_attributes"]["sender_name"],
                    "vendor_id": result1["all_attributes"]["vendor_id"],
                    "vendor_name": result1["all_attributes"]["vendor_name"],
                    "description": result1["all_attributes"]["description"],
                    "status": result1["all_attributes"]["status"]
                }]
                conct.close_connection(client)
                return {'statusCode': 200, 'body': lst}
            else:
                conct.close_connection(client)
                return {'statusCode': 200, 'body': []}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

    def cmsPurchaseReturnEdit(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            return_id = data["return_id"]
            poData = list(db_con.PurchaseReturn.find({"all_attributes.return_id": return_id}))
            if poData:
                pk_id_for_po = poData[0]["pk_id"]
            all_attributes = poData[0]["all_attributes"]
            all_attributes['status'] = data['status']
            all_attributes['description'] = data['description']
            db_con.PurchaseReturn.update_one(
                {"pk_id": pk_id_for_po},
                {"$set": {"all_attributes": all_attributes}}
            )
            return {"statusCode": 200, "body": "po return Details updated successfully"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

    def cmsPurchaseOrderGetPurchaseReturnList(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            databaseTableName = "PtgCms" + data["env_type"]
            #print(databaseTableName)
            result = list(db_con.PurchaseReturn.find({}))
            if result:
                lst = [
                    {
                        "status": i["all_attributes"]["status"] if "status" in i["all_attributes"] else " ",
                        "return_id": i["all_attributes"]["return_id"] if "return_id" in i["all_attributes"] else " ",
                        "vendor_id": i["all_attributes"]["vendor_id"] if "vendor_id" in i["all_attributes"] else " ",
                        "return_date": i["sk_timeStamp"][:10],
                        "return_value": sum(float(part['price']) for part in i["all_attributes"]["parts"].values()),

                        "order_price": i["all_attributes"]["total_price"] if "total_price" in i[
                            "all_attributes"] else " ",
                        "order_date": i["all_attributes"]["order_date"][:10] if "order_date" in i[
                            "all_attributes"] else " ",
                    }
                    for i in result
                ]

                lst = sorted(lst, key=lambda x: x['return_id'], reverse=True)
                return {'statusCode': 200, 'body': lst}
            else:
                return {'statusCode': 200, 'body': []}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

   
    def cmsNewPurchaseReturnGetOrderId(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sorted_pk_ids1 = list(db_con.NewPurchaseOrder.find({ "gsipk_table":"PurchaseOrder"},{"_id":0}))
            if sorted_pk_ids1:
                sorted_pk_ids = sorted([item["all_attributes"]['po_id'] for item in sorted_pk_ids1], reverse=True)
                sorted_pk_ids2 = sorted(sorted_pk_ids, key=lambda x: int(x.split("/")[2])) 
                conct.close_connection(client)
                return {"statusCode": 200, "body": sorted_pk_ids2}
            else:
                conct.close_connection(client)
                return {"statusCode": 404, "body": "No Data"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

    def cmsNewPurchaseReturnGetInwardId(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            inwardId = data['po_order_id']
            qaData = list(db_con.QATest.find({"all_attributes.document_id": inwardId},
                                             {"all_attributes.inwardId": 1, "all_attributes.parts": 1,'_id':0}))
            if qaData:
                a = [item['all_attributes']['inwardId'] for item in qaData if
                     any(int(k['fail_qty']) > 0 for j, k in item['all_attributes']['parts'].items())]
                #print(a)
                sorted_pk_ids2 = sorted(a, key=lambda x: int(x.split('_')[1][2:]))
                conct.close_connection(client)
                return {"statusCode": 200, "body": sorted_pk_ids2}
            else:
                conct.close_connection(client)
                return {"statusCode": 404, "body": "No Data"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}

    def cmsNewGetComponentDetailsInsidePurchaseReturnModified(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            po_id = data['po_order_id']
            # print(po_id)
            inwardId = data['inwardId']
            sorted_pk_ids_list = list(
                db_con.QATest.find({"all_attributes.inwardId": inwardId, "all_attributes.document_id": po_id},{'_id':0}))
            category_data = list(
                db_con.Metadata.find({"gsipk_id": "Electronic"}, {"gsisk_id": 1, "sub_categories": 1, "pk_id": 1}))
            category_data = {item['pk_id'].replace("MDID", "CTID"): {"ctgr_name": item['gsisk_id'],
                                                                     "sub_categories": item['sub_categories']} for item
                             in category_data}
            inventory = list(db_con.Inventory.find({}, {"all_attributes.description": 1, "all_attributes.package": 1,
                                                        "all_attributes.manufacturer": 1, "all_attributes.cmpt_id": 1,
                                                        "all_attributes.prdt_name": 1, "all_attributes.sub_ctgr": 1}))
            po_data = list(db_con["NewPurchaseOrder"].find({"gsipk_table": "PurchaseOrder", "all_attributes.po_id": po_id},{'_id':0}))

            inventory = {item['all_attributes']['cmpt_id']: item['all_attributes'] for item in inventory}
            if sorted_pk_ids_list:
                filtered_list = []
                for item in sorted_pk_ids_list:
                    total_basic_amount = 0
                    total_basic_amount_gst = 0
                    grand_total = 0
                    parts = {}

                    for part_key, part_data in item['all_attributes']['parts'].items():
                        if int(part_data['fail_qty']) > 0:
                            price = float(part_data['price_per_piece']) * float(part_data['fail_qty'])
                            total_basic_amount += price
                            gst_amount = (price * float(part_data.get('gst', 0)) / 100)  
                            total_basic_amount_gst += gst_amount
                            grand_total += price + gst_amount  
                
                            
                            po_item = next(
                                (po for po in po_data[0]['all_attributes']['purchase_list'].values()
                                if part_data['cmpt_id'] == po.get('cmpt_id', '')),
                                {}
                            )
                            
                            parts[part_key] = {
                                "cmpt_id": part_data['cmpt_id'],
                                "ctgr_id": part_data['ctgr_id'],
                                "price_per_piece": part_data['price_per_piece'],
                                "description": part_data['description'],
                                "packaging": part_data['packaging'],
                                "fail_qty": part_data['fail_qty'],
                                "batchId": part_data['batchId'],
                                "mfr_prt_num": part_data['mfr_prt_num'],
                                "manufacturer": part_data['manufacturer'],
                                "price": float(part_data['price_per_piece']) * float(part_data['fail_qty']),
                                "pass_qty": part_data['pass_qty'],
                                "qty": part_data['qty'],
                                "gst": po_item.get('gst', part_data['gst']),
                                "gst_amount": gst_amount,
                                "item_no": po_item.get('item_no', ""),
                                "rev": po_item.get('rev', ""),
                                "unit": po_item.get('unit', ""),
                                "delivery_date": po_item.get('delivery_date', ""),
                                "department": part_data['department'],
                                "prdt_name": category_data[part_data['ctgr_id']]['sub_categories'][inventory[part_data['cmpt_id']]['sub_ctgr']]
                                if part_data['department'] == 'Electronic' else part_data['prdt_name']
                            }

                    filtered_list.append({
                        "From": po_data[0]['all_attributes']['ship_to'],
                        "To": po_data[0]['all_attributes']['kind_attn'],
                        "req_line": po_data[0]['all_attributes']['req_line'],
                        "po_terms_conditions": po_data[0]['all_attributes']['po_terms_conditions'],
                        "outpass_upload": "",
                        "total_amount": 
                           {
                                "total_basic_amount": int(total_basic_amount),
                                "total_basic_amount_gst": total_basic_amount_gst,
                                "grand_total": grand_total
                            
                        },
                        "parts": parts
                    })
          

                    conct.close_connection(client)
                    return {"statusCode": 200, "body": filtered_list[0]}
                else:
                    conct.close_connection(client)
                    return {"statusCode": 200, "body": "we cant return fail qty is zero"}

            else:
                conct.close_connection(client)
                return {"statusCode": 404, "body": "No Data"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
    def CmsNewPurchaseReturnCreate(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            all_attributes = {}
            po_id = data['po_id']
            # gsipk_table = "PurchaseReturn"
            # description = data['description']
            inward_id = data['inward_id']
            result1 =list(db_con["NewPurchaseOrder"].find({"gsipk_table": "PurchaseOrder", "all_attributes.po_id": po_id},{'_id':0}))

            result3 = list(db_con.QATest.find({"all_attributes.inwardId": inward_id}))
            parts = result3[0]['all_attributes']['parts']
            matching_parts = {f"part{index + 1}": value for index, part in enumerate(data['parts']) for key, value in
                              parts.items() if value['mfr_prt_num'] == part['mfr_prt_num']}
            result = list(db_con.PurchaseReturn.find({"gsisk_id": po_id,"gsipk_table":"NewPurchaseReturn"},{"_id":0}))
            return_id = f"{data['po_id']}-R1"
            if result:
                if any(item['gsipk_id'] == data['inward_id'] for item in result):
                    return {'statusCode': 400, 'body': "Return already present for given inward id"}
                returnids = sorted([int(item["pk_id"].split("R")[-1]) for item in result], reverse=True)
                print(returnids)
                return_id = f"{data['po_id']}-R{returnids[0] + 1}"
            sk_timeStamp = (datetime.now()).isoformat()
            parts = {f"part{inx + 1}": value for inx, value in enumerate(data['parts'])}

            attachment_list = []
            primary_document_details=data['primary_document_details']
            if "outpass_upload" in data['primary_document_details']:
                for attachment in data['primary_document_details']['outpass_upload']:
                    document_body = attachment['doc_body']
                    document_name = attachment['doc_name']
                    extra_type = ""
                    
                    attachment_url = file_uploads.upload_file(
                        "NewPurchaseReturn",
                        "PtgCms" + env_type,
                        extra_type,
                        "NewPo" + po_id,
                        document_name,
                        document_body
                    )
                    
                    attachment_list.append({
                        "doc_body": attachment_url,
                        "doc_name": document_name
                    })

            primary_document_details['outpass_upload'] = attachment_list

            if result1:
                vendor_id = [i['all_attributes']['vendor_id'] for i in result1]
                #print(vendor_id)
                result2 = list(db_con.Vendor.find({"pk_id": vendor_id[0]}))
                #print(result2)
                if result2:
                    if parts:
                        all_attributes = {}
                        all_attributes['return_id'] = return_id
                        all_attributes['parts'] = parts
                        all_attributes['From'] = data['From']
                        all_attributes['To'] = data['To']
                        all_attributes['req_line'] = data['req_line']
                        all_attributes['note'] = data['note']
                        all_attributes['primary_document_details'] = primary_document_details
                        all_attributes['total_amount'] = data['total_amount']
                        all_attributes['inward_id'] = inward_id

                        # all_attributes=create_nested_dict(all_attributes)
                        item = {
                            "pk_id": return_id,
                            "sk_timeStamp": sk_timeStamp,
                            "all_attributes": all_attributes,
                            "gsipk_table": "NewPurchaseReturn",
                            "gsisk_id": data['po_id'],
                            "gsipk_id": data['inward_id'],
                            "lsi_key": "--"
                        }
                        updat = db_con.PurchaseReturn.insert_one(item)
                        for i, j in matching_parts.items():
                            cmpt_id = j['cmpt_id']
                            result4 = list(db_con.Inventory.find({"pk_id": cmpt_id}, {"all_attributes.rtn_qty": 1,
                                                                                      "all_attributes.cmpt_id": 1,
                                                                                      "pk_id": 1, "sk_timeStamp": 1}))
                            if result4:
                                inventory_item = result4[0]
                                if 'rtn_qty' in inventory_item:
                                    existing_qty = int(inventory_item['all_attributes']['rtn_qty'])
                                else:
                                    existing_qty = 0
                                new_fail_qty = int(j['fail_qty'])
                                total_qty = existing_qty + new_fail_qty
                                resp = db_con.Inventory.update_one(
                                    {"pk_id": inventory_item['pk_id']},
                                    {"$set": {"all_attributes.rtn_qty": str(total_qty)}}
                                )
                        try:
                            response = {
                                'statusCode': 200,
                                'body': 'Purchase Return added successfully'
                            }
                            return response

                        except Exception as err:
                            exc_type, exc_obj, tb = sys.exc_info()
                            f_name = tb.tb_frame.f_code.co_filename
                            line_no = tb.tb_lineno
                            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
                            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
                    else:
                        return {'statusCode': 204, 'body': "parts are not added please check"}
                else:
                    return {'statusCode': 404, 'body': "vendors data is not there please check"}
            else:
                return {'statusCode': 404, 'body': "po data is not there"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
    def cmsNewPurchaseOrderGetPurchaseReturnList(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            result = list(db_con.PurchaseReturn.find({"gsipk_table":"NewPurchaseReturn"},{"_id":0}))
            if result:  
             response = [{
 
                "return_id":i['pk_id'],
                "company_name": i["all_attributes"]["To"]['company_name'],
                "document_title": i["all_attributes"]["primary_document_details"]['document_title'],
                "req_status":i["all_attributes"]["primary_document_details"]['status'],
                "last_modified_date": i["sk_timeStamp"]
             }
              for i in result
             ]
            lst = sorted(response, key=lambda x: x['return_id'], reverse=True)
            return {"statusCode": 200, "body": lst}
       
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
    def cmsGetPdfNewPurchaseOrderReturn(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            return_id=data['return_id']
            result = list(db_con.PurchaseReturn.find({"gsipk_table":"NewPurchaseReturn","pk_id":return_id},{"_id":0,"all_attributes":1,"gsisk_id":1}))
            if result:
                response_data = result[0]['all_attributes']
                response_data['po_id'] = result[0]['gsisk_id']
                parts_dict = response_data.get('parts', {})
                response_data['parts'] = list(parts_dict.values())
                return {"statusCode": 200, "body": response_data}
            else:
                return {"statusCode": 200, "body": "No Data Found For Given Return Id"}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
    def cmsPurchaseReturnUpdateCommentsAndAttachments(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            return_id = data["return_id"]
            time_atch = datetime.now().isoformat()
            result = list(db_con["PurchaseReturn"].find({"gsipk_table": "NewPurchaseReturn", "pk_id": return_id},{'_id':0}))
            if result:
                attachment_list=[]
                all_attributes = result[0]["all_attributes"]
                pk_id = result[0]["pk_id"]
                sk_timeStamp = result[0]["sk_timeStamp"]
                comment_details = {
                    "comment": data.get("comment", ""),
                    "doc_time": time_atch
                }
                all_attributes.setdefault("cmts_atcmts", {})
                if "attachment" in data and data["attachment"]:
                    for attachment in data['attachment']:
                        document_body = attachment['doc_body']
                        document_name = attachment['doc_name']
                        extra_type = ""
                        
                        attachment_url = file_uploads.upload_file(
                            "NewPurchaseReturn",
                            "PtgCms" + env_type,
                            extra_type,
                            "NewPr" + pk_id,
                            document_name,
                            document_body
                        )
                        attachment_list.append({
                            "doc_body": attachment_url,
                            "doc_name": document_name
                        })
                        
                    existing_keys = all_attributes["cmts_atcmts"].keys()
                    max_index = max([int(k.split("_")[-1]) for k in existing_keys], default=0)
                    next_index = max_index + 1
                    all_attributes["cmts_atcmts"][f"cmts_atcmts_{next_index}"] = {
                        "comment": comment_details["comment"],
                        "doc_time": comment_details["doc_time"],
                        "Attachments": 
                            attachment_list,
                    }

                else:
                    existing_keys = all_attributes["cmts_atcmts"].keys()
                    max_index = max([int(k.split("_")[-1]) for k in existing_keys], default=0)
                    next_index = max_index + 1
                    all_attributes["cmts_atcmts"][f"cmts_atcmts_{next_index}"] = {
                        "comment": comment_details["comment"],
                        "doc_time": comment_details["doc_time"]
                    }

                # return all_attributes
                db_con["PurchaseReturn"].update_one(
                    {"pk_id": pk_id},
                    {"$set": {"all_attributes": all_attributes}}
                )
                return {"statusCode": 200, "body": "Comment added successfully"}
            else:
                return {"statusCode": 404, "body": "No  purchase order found"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request (check event)'}
    def cmsGetCommentsAndAttachmentsForPurchaseReturn(request_body):    
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            return_id=data['return_id']
            a={}
            result = list(db_con.PurchaseReturn.find({'pk_id':return_id, "gsipk_table":"NewPurchaseReturn"},{'_id':0,'all_attributes':1,"sk_timeStamp":1,"gsisk_id":1}))
            dates = [datetime.strptime(part['delivery_date'], "%Y-%m-%d") for part in result[0]['all_attributes']['parts'].values() if 'delivery_date' in part]
            if 'cmts_atcmts' in result[0]['all_attributes']:
                a['cmts_atcmts'] = result[0]['all_attributes']['cmts_atcmts']
                a['comment_count'] = sum(
                    1 for c in result[0]['all_attributes']['cmts_atcmts'].values() if isinstance(c, dict) and c.get('comment')
                )
                a['attachments_count'] = sum(
                    len(c.get('Attachments', {})) for c in result[0]['all_attributes']['cmts_atcmts'].values() if isinstance(c, dict)
                )
            else:
                a['comment_count'] = 0  
                a['attachments_count'] = 0
            a['grand_total'] = result[0]['all_attributes']['total_amount']['grand_total']
            a['parts_count'] = len(result[0]['all_attributes']['parts'])
            a['po_id']=result[0]['gsisk_id']
            dates = [datetime.strptime(part['delivery_date'], "%Y-%m-%d") 
                    for part in result[0]['all_attributes']['parts'].values() 
                    if 'delivery_date' in part]
            a['max_delivery_date'] = max(dates).strftime("%d/%m/%Y") if dates else None

            sk_timestamp=result[0]['sk_timeStamp']
            formatted_date = datetime.strptime(sk_timestamp, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d,%I:%M %p")
            a['po_return_created_date'] = formatted_date  

            return {'statuscode':200,'body':a}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Something went wrong'}
    def cmsNewPurchaseReturnEdit(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            return_id = data["return_id"]
            poData = list(db_con.PurchaseReturn.find({"pk_id":return_id,"gsipk_table":"NewPurchaseReturn"},{'_id':0}))
            if poData:
                pk_id_for_po = poData[0]["pk_id"]
            all_attributes = poData[0]["all_attributes"]
            all_attributes['To']=data['To']
            all_attributes['req_line']=data['req_line']
            all_attributes['primary_document_details']['status']=data['status']
            db_con.PurchaseReturn.update_one(
                {"pk_id": pk_id_for_po},
                {"$set": {"all_attributes": all_attributes}}
            )
            return {"statusCode": 200, "body": "po return Details updated successfully"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
    def cmsNewPurchaseReturnEditGetDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            return_id = data['return_id']
            result = list(db_con.PurchaseReturn.find({"all_attributes.return_id": return_id,"gsipk_table":"NewPurchaseReturn"},
                                                     {"all_attributes": 1, "sk_timeStamp": 1,"_id":0}))
            result1 = result[0]
            if result1:
                lst = {
                    "inward_id": result1["all_attributes"]["inward_id"],
                    "To": result1["all_attributes"]["To"],
                    "parts": result1["all_attributes"]["parts"],
                    "From":result1['all_attributes']["From"],
                    "note": result1["all_attributes"]["note"],
                    "return_id": result1["all_attributes"]["return_id"],
                    "primary_document_details": result1["all_attributes"]["primary_document_details"],
                    "req_line": result1["all_attributes"]["req_line"],
                    "total_amount":result1["all_attributes"]["total_amount"]
                }
                conct.close_connection(client)
                return {'statusCode': 200, 'body': lst}
            else:
                conct.close_connection(client)
                return {'statusCode': 200, 'body': []}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}