
import json
from datetime import datetime, timedelta,date
import base64
from db_connection import db_connection_manage
import sys
import os
from dateutil.relativedelta import relativedelta
from bson import ObjectId
from cms_utils import file_uploads

conct = db_connection_manage()
fiup = file_uploads()
class ProformaInvoice():
    def cmsproformaInvoicegetClientDetails(request_body):
        try:
            data = request_body
            print(request_body)
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            client_id=data['client_id']
            sk_timeStamp = (datetime.now()).isoformat()
            client_db = list(db_con.Clients.find({"all_attributes.client_id":client_id},{'_id':0}))
            if client_db:
                result=[{"client_id":i['all_attributes']['client_id'],
                    "client_name":i['all_attributes']['client_name'],
                    "kind_attn":{"gst_number":"",
                                    "pan_number":"",
                                    "contact_number":i['all_attributes']['contact_number'],
                                    "contact_details":i['all_attributes']['contact_number'],
                                    "adress":""},
                    "ship_to":{"gst_number":"",
                                    "pan_number":"",
                                    "contact_number":i['all_attributes']['contact_number'],
                                    "contact_details":i['all_attributes']['contact_number'],
                                    "adress":""},
                    "request_line":"Dear Sir/Ma'am,We acknowledge with thanks in receipt of your above-mentioned purchase order for supplyof below parts In this connection we are pleased to submit our Proforma Invoice as below"
                                    } for i in client_db]
            else:
                return {'statusCode':404,'body':"No data for this client"}
            return {'statusCode':200,'body':result[0]}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Internal server error'}

    def cmsGetFCPOId(request_body):    
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            query = {
                "gsipk_table":"ForcastPurchaseOrder",
                "gsisk_id":"open",
                "lsi_key":"pending"
            }
            projection = {
                "all_attributes.fcpo_id": 1,
                "_id": 0
            }
            result = db_con.ForcastPurchaseOrder.find(query, projection)
            data = [
                {"forecast_po": i["all_attributes"]["fcpo_id"]} 
                for i in result
                ]
            return {'statusCode':200,'body':data}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
           # print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Internal server error'}
        
        
    def CmsActiveForcastPurchaseorderDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            query = {
                "gsipk_table":"ForcastPurchaseOrder",
                "gsisk_id":"open"
                #"lsi_key":"pending"//rejected/approved/cancel state are also there consider it.
            }
            projection = {
                "all_attributes.fcpo_id": 1,
                "_id": 0
            }
            result = db_con.ForcastPurchaseOrder.find(query, projection)
            data = [
                {"forecast_po": i["all_attributes"]["fcpo_id"]} 
                for i in result
                ]
            return {'statusCode':200,'body':data}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
           # print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Internal server error'}     


    def cmsSaveFCPOCardDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            pi_id = data['fcpo_id']
            
            attachment_dict = {}
            if "attachment" in data.keys() and data["attachment"]:
                for attachment in data['attachment']:
                    document_body = attachment['doc_body']
                    document_name = attachment['doc_name']
                    extra_type = ""
                    attachment_url = file_uploads.upload_file("ForcastPurchaseOrder", "PtgCms" + env_type, extra_type, "ForcastPurchaseOrder" + pi_id, document_name, document_body)
                    attachment_dict[document_name] = attachment_url  # Adds or updates the dictionary entry for each attachment
            else:
                attachment_dict = {}
            comment_data = {
                "created_time": datetime.now().isoformat(),
                "comment": data["comment"],
                "attachments": attachment_dict  # This dictionary now contains all attachments
            }
            gate_entry_result = db_con.ForcastPurchaseOrder.find_one({"all_attributes.fcpo_id": pi_id}, {"_id": 0, "all_attributes": 1})
            forecast_details = gate_entry_result.get("all_attributes", {}).get("forecast_details", {})
            forecastDetails = {f"forecast{inx + 1}": value for inx, value in enumerate(forecast_details.items())}

            comments_dict = gate_entry_result.get("all_attributes", {}).get("forecast_details", {}).get("forecast1","").get("cmts_atcmts")
            print(f"The comments{comments_dict}")
            if not isinstance(comments_dict, dict):
                if isinstance(comments_dict, list):
                    comments_dict = {f"comment{i + 1}": v for i, v in enumerate(comments_dict)}
                else:
                    comments_dict = {}
            new_comment_key = f"comment{len(comments_dict) + 1}"
            # Add the new comment to the dictionary
            comments_dict[new_comment_key] = comment_data
            
            db_con.ForcastPurchaseOrder.update_one(
                    {"all_attributes.fcpo_id": pi_id},
                    {"$set": {"all_attributes.forecast_details.forecast1.cmts_atcmts": comments_dict}}
            )
            result = list(db_con.ForcastPurchaseOrder.find({"fcpo_id": pi_id}, {"_id": 0}))
            return{'statusCode': 200, 'body': 'Comment added successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Comment adding failed'}  



    def cmsSaveProformaInvoiceCardDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            pi_id = data['pi_id']
            
            attachment_dict = {}
            if "attachment" in data.keys() and data["attachment"]:
                for attachment in data['attachment']:
                    document_body = attachment['doc_body']
                    document_name = attachment['doc_name']
                    extra_type = ""
                    attachment_url = file_uploads.upload_file("ProformaInvoice", "PtgCms" + env_type, extra_type, "ProformaInvoice" + pi_id, document_name, document_body)
                    attachment_dict[document_name] = attachment_url  # Adds or updates the dictionary entry for each attachment
            else:
                attachment_dict = {}
            comment_data = {
                "created_time": datetime.now().isoformat(),
                "comment": data["comment"],
                "attachments": attachment_dict  # This dictionary now contains all attachments
            }
            gate_entry_result = db_con.ProformaInvoice.find_one({"all_attributes.pi_id": pi_id}, {"_id": 0, "all_attributes": 1})
            comments_dict = gate_entry_result.get('all_attributes', {}).get('comments', {})
            if not isinstance(comments_dict, dict):
                if isinstance(comments_dict, list):
                    comments_dict = {f"comment{i + 1}": v for i, v in enumerate(comments_dict)}
                else:
                    comments_dict = {}
            new_comment_key = f"comment{len(comments_dict) + 1}"
            # Add the new comment to the dictionary
            comments_dict[new_comment_key] = comment_data
            db_con.ProformaInvoice.update_one(
                    {"all_attributes.pi_id": pi_id},
                    {"$set": {"all_attributes.comments": comments_dict}}
            )
            result = list(db_con.ProformaInvoice.find({"pi_id": pi_id}, {"_id": 0}))
            return{'statusCode': 200, 'body': 'Comment added successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Comment adding failed'}  


    def cmsGetProformaInvoiceCardDetails(request_body): 
        try:
            #"-----use this api for fetching the saved details of the PI-----" 
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            collection_name = "ProformaInvoice"
            pk_id = data["pi_id"]
            
            # Query MongoDB for the forecast purchase order details
            
            if "PI" in pk_id:
             result = list(db_con[collection_name].find({"gsipk_table": collection_name, "all_attributes.pi_id": pk_id}))

            elif "FCPO" in pk_id:
             result = list(db_con["ForcastPurchaseOrder"].find({"gsipk_table": "ForcastPurchaseOrder", "all_attributes.fcpo_id": pk_id}))

            elif "CFPO" in pk_id:
             result = list(db_con[collection_name].find({"gsipk_table": collection_name, "all_attributes.primary_document_details.client_po": pk_id}))
            
            
            if result:
               
                # Final response format with bom_name and client_name at the top level
                if "PI" in pk_id:
                 comments = result[0]["all_attributes"].get("comments","")
                elif "FCPO" in pk_id:
                 comments = result[0]["all_attributes"].get("forecast_details","").get("forecast1","").get("cmts_atcmts")
                comments_data_list = []
                attachment_count = 0
                if comments:
                    for comment_key,comment in comments.items():
                        attachments_data_list = []
                        attachments_data = comment.get('attachments', [])
                        if attachments_data:
                            for attachment_key, attachment in attachments_data.items():
                                attachment_count = attachment_count + 1
                                if "PI" in pk_id:
                                 attachments = {
                                    'file_name': attachment_key,
                                    'file_url': attachment
                                }
                                elif "FCPO" in pk_id:
                                      attachments = {
                                   'file_name': attachment_key,
                                   'file_url': attachment
                                }
                                attachments_data_list.append(attachments)
                        if "PI" in pk_id:        
                         comment_data = {
                            "comment_id": comment_key,
                            "comment_text": comment.get('comment', ''),
                            "created_time": datetime.fromisoformat(comment.get('created_time', '')).strftime("%d/%m/%Y, %I:%M %p"),
                            "attachments": attachments_data_list
                        }
                        elif "FCPO" in pk_id:
                            comment_data = {
                            "comment_id": comment_key,
                            "comment_text": comment.get('comment', ''),
                            #"created_time": datetime.fromisoformat(comment.get('created_time', '')).strftime("%d/%m/%Y, %I:%M %p"),
                            "attachments": attachments_data_list
                        } 
                        comments_data_list.append(comment_data)
                if "PI" in pk_id:        
                 response = {
                    "statusCode": 200,
                    "body": {
                        "pi_number" : result[0]["all_attributes"].get("pi_id",""),
                        "pi_date":result[0]["all_attributes"].get("primary_document_details","").get("PI_date",""),
                        "amount" :result[0]["all_attributes"].get("total_amount","").get("grand_total","") ,
                        "number_of_item": len(result[0]["all_attributes"]["products_list"]),
                        "create_date" :  datetime.fromisoformat(result[0]["sk_timeStamp"]).strftime("%d/%m/%Y, %I:%M %p"),
                        "due_date":result[0]["all_attributes"].get("primary_document_details","").get("Pi_date",""),
                        "status":result[0]["all_attributes"].get("primary_document_details","").get("status",""),
                        "fcpo": result[0]["all_attributes"].get("primary_document_details","").get("fcpo","") ,
                        "client_po":result[0]["all_attributes"].get("primary_document_details","").get("client_po",""),
                        "transction_name":result[0]["all_attributes"].get("primary_document_details","").get("transaction_name",""),
                        "comments":comments_data_list,
                    }
                }
                elif "FCPO" in pk_id:
                    response = {
                    "statusCode": 200,
                    "body": {
                        "pi_number" : result[0]["all_attributes"].get("pi_id",""),
                        "pi_date":result[0]["all_attributes"].get("primary_document_details","").get("Pi_date",""),
                        "amount" :"" ,
                        "number_of_item": "",
                        "create_date" :  datetime.fromisoformat(result[0]["sk_timeStamp"]).strftime("%d/%m/%Y, %I:%M %p"),
                        "due_date":result[0]["all_attributes"].get("primary_document_details","").get("Pi_date",""),
                        "status":result[0]["all_attributes"].get("primary_document_details","").get("status",""),
                        "fcpo": result[0]["all_attributes"].get("fcpo_id","") ,
                        "client_po":result[0]["all_attributes"].get("primary_document_details","").get("client_po",""),
                        "transction_name":result[0]["all_attributes"].get("primary_document_details","").get("transaction_name",""),
                        "comments":comments_data_list,
                    }
                }

                return response
            else:
                {"statusCode":404,"message":"data not available"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Internal server error'}            
        
    def cmsproformaInvoiceCreate(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            #client_id=data['client_id']
            primary_doc_details = data.get("primary_document_details", {})
            PI_date = primary_doc_details.get("PI_date", "")
            sk_timeStamp = (datetime.now()).isoformat()

            if PI_date:
                try:
                    pi_date_obj = datetime.strptime(PI_date, "%Y-%m-%d")
                    pi_month_year = f"{pi_date_obj.strftime('%m/%y')}-{(pi_date_obj.year + 1) % 100:02}"
                    print(pi_month_year)

                except ValueError:
                    return {'statusCode': 400, 'body': 'Invalid SO_Date format'}
            else:
                return {'statusCode': 400, 'body': 'PI_Date is required'}
            
            proforma_data = list(db_con.ProformaInvoice.find({}))
            proforma_order_id = "1"
            client_PI_num = f"EPL/PI/1/{pi_month_year}"

            if proforma_data:
                update_id = list(db_con.all_tables.find({"pk_id": "top_ids"},{"_id":0}))
                if "ProformaInvoice" in update_id[0]['all_attributes']:
                    update_id = (update_id[0]['all_attributes']['ProformaInvoice'][5:])
                else:
                    update_id = "1"
                proforma_order_id = str(int(update_id) + 1)
            PI_id = [i["all_attributes"]["pi_id"] for i in proforma_data if "pi_id" in i["all_attributes"]]
            if PI_id:
                client_PI_num = f"EPL/PI/{proforma_order_id}/{pi_month_year}"

            Proforma_data = {
                "pk_id": "PIPTG" + proforma_order_id,
                "sk_timeStamp": sk_timeStamp,
                "all_attributes": {
                    "ship_to": data.get("ship_to", {}),
                    "req_line": data.get("req_line", ""),
                    "pi_terms_conditions": data.get("pi_terms_conditions", ""),
                    "kind_attn": data.get("kind_attn", {}),
                    "primary_document_details": primary_doc_details,
                    "products_list": data.get("product_list", {}),
                    "total_amount": data.get("total_amount", {}),
                    "secondary_doc_details": data.get("secondary_doc_details", {}),
                    "pi_id": client_PI_num,
                    "client_id": data.get("client_id", ""),
                    # "forecast_id":data.get('forecase_id',"")
                },
                "gsisk_id": "open",
                "gsipk_table": "ProformaInvoice",
                "lsi_key": ""
                
            }

            db_con.ProformaInvoice.insert_one(Proforma_data)
            key = {'pk_id': "top_ids", 'sk_timeStamp': "123"}        
            update_data = {
                '$set': {
                    'all_attributes.ProformaInvoice': "PIPTG" + proforma_order_id
                }
            }
            db_con.all_tables.update_one(key, update_data)
            conct.close_connection(client)
            return {'statusCode': 200, 'body': 'New PI created successfully'}



        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Internal server error'}
    def CmsEditProformaInvoice(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            updatestatus = data["updatestatus"]
            PI_no=data["pi_id"]
            sk_timeStamp = (datetime.now()).isoformat()
            pi_data = db_con.ProformaInvoice.find_one({"all_attributes.pi_id": PI_no, "lsi_key": updatestatus})
            if not pi_data:
                return {'statusCode': 404, 'body': 'Service Order not found'}
            new_status = "Pending" if updatestatus == "Rejected" else updatestatus
            update_fields = {
                "ship_to": data.get("ship_to", pi_data["all_attributes"].get("ship_to", {})),
                "req_line": data.get("req_line", pi_data["all_attributes"].get("req_line", "")),
                "pi_terms_conditions": data.get("pi_terms_and_conditions", pi_data["all_attributes"].get("pi_terms_conditions", "")),
                "kind_attn": data.get("kind_attn", pi_data["all_attributes"].get("kind_attn", {})),
                "primary_document_details": data.get("primary_document_details", pi_data["all_attributes"].get("primary_document_details", {})),
                "products_list": data.get("products_list", pi_data["all_attributes"].get("products_list", {})),
                "total_amount": data.get("total_amount", pi_data["all_attributes"].get("total_amount", {})),
                "secondary_doc_details": data.get("secondary_doc_details", pi_data["all_attributes"].get("secondary_doc_details", {})),
                "client_id": data.get("client_id", pi_data["all_attributes"].get("client_id", "")),
                "pi_id": PI_no 
            }

            # Update the service order in the database
            db_con.ProformaInvoice.update_one(
                {"all_attributes.pi_id": PI_no},
                {"$set": {
                    "sk_timeStamp": sk_timeStamp,
                    "all_attributes": update_fields,
                    "lsi_key": new_status
                }}
            )

            conct.close_connection(client)
            return {'statusCode': 200, 'body': 'Proforma Invoice updated successfully'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'internal server error'}  
        
    
    def proformaInvoiceEditGet(request_body):
        try:
            data = request_body
            print(request_body)
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            pi_id = data.get('pi_id', '')

            if pi_id:
                proforma_record = db_con.ProformaInvoice.find_one({"all_attributes.pi_id": pi_id}, {"_id": 0, "all_attributes": 1})
                if proforma_record:
                    return {'statusCode': 200, 'body': proforma_record['all_attributes']}
                else:
                    proforma_record1 = db_con.DraftProformaInvoice.find_one({"all_attributes.pi_id": pi_id}, {"_id": 0, "all_attributes": 1})
                    if proforma_record1:
                        return {'statusCode': 200, 'body': proforma_record1['all_attributes']}
                    else:
                        return {'statusCode': 400, 'body': 'No record found for this pi id'}
            else:
                return {'statusCode': 400, 'body': 'PI ID is required'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Internal server error'}


    # def proformaInvoiceEditGet(request_body):
    #     try:
    #         data = request_body
    #         print(request_body)
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         client = db_conct['client']
    #         pi_id = data['pi_id']
    #         proforma_record = db_con.ProformaInvoice.find_one({"all_attributes.pi_id":pi_id},{"_id":0,"all_attributes":1})
    #         print(proforma_record)
    #         if proforma_record:
    #             return {'statusCode': 400,'body': proforma_record['all_attributes']}
    #         else:
    #             return {'statusCode': 400,'body': 'No record found for this pi id'}
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400,'body': 'Internal server error'}
    def cmsproformaInvoiceSaveDraft(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            client_id=data['client_id']
            primary_doc_details = data.get("primary_document_details", {})
            PI_date = primary_doc_details.get("PI_date", "")
            sk_timeStamp = (datetime.now()).isoformat()

            if PI_date:
                try:
                    pi_date_obj = datetime.strptime(PI_date, "%Y-%m-%d")
                    pi_month_year = f"{pi_date_obj.strftime('%m/%y')}-{(pi_date_obj.year + 1) % 100:02}"
                    print(pi_month_year)

                except ValueError:
                    return {'statusCode': 400, 'body': 'Invalid draft pi_Date format'}
            else:
                return {'statusCode': 400, 'body': 'DRaft_PI_Date is required'}
            
            proforma_data = list(db_con.DraftProformaInvoice.find({}))
            proforma_order_id = "1"
            client_PI_num = f"EPL/DPI/1/{pi_month_year}"

            if proforma_data:
                update_id = list(db_con.all_tables.find({"pk_id": "top_ids"},{"_id":0}))
                if "DraftProformaInvoice" in update_id[0]['all_attributes']:
                    update_id = (update_id[0]['all_attributes']['DraftProformaInvoice'][6:])
                else:
                    update_id = "1"
                proforma_order_id = str(int(update_id) + 1)
            PI_id = [i["all_attributes"]["pi_id"] for i in proforma_data if "pi_id" in i["all_attributes"]]
            if PI_id:
                client_PI_num = f"EPL/DPI/{proforma_order_id}/{pi_month_year}"

            Proforma_data = {
                "pk_id": "DPIPTG" + proforma_order_id,
                "sk_timeStamp": sk_timeStamp,
                "all_attributes": {
                    "ship_to": data.get("ship_to", {}),
                    "req_line": data.get("req_line", ""),
                    "pi_terms_conditions": data.get("pi_terms_conditions", ""),
                    "kind_attn": data.get("kind_attn", {}),
                    "primary_document_details": primary_doc_details,
                    "products_list": data.get("product_list", {}),
                    "total_amount": data.get("total_amount", {}),
                    "secondary_doc_details": data.get("secondary_doc_details", {}),
                    "pi_id": client_PI_num,
                    "client_id": data.get("client_id", ""),
                    "forecast_id":data.get("forecast_id","")
                },
                "gsisk_id": "open",
                "gsipk_table": "DraftProformaInvoice",
                "lsi_key": ""
                
            }

            db_con.DraftProformaInvoice.insert_one(Proforma_data)
            key = {'pk_id': "top_ids", 'sk_timeStamp': "123"}        
            update_data = {
                '$set': {
                    'all_attributes.DraftProformaInvoice': "DPIPTG" + proforma_order_id
                }
            }
            db_con.all_tables.update_one(key, update_data)
            conct.close_connection(client)
            return {'statusCode': 200, 'body': 'New Draft PI created successfully'}



        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Internal server error'}
        


    def cmsPOGetProformaInvoice(request_body):
        try:
            data = request_body #test
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            po_id = data['po_id']
            result = list(db_con.NewPurchaseOrder.find({'all_attributes.po_id': po_id, "gsipk_table": "PurchaseOrder"},{'_id': 0, 'all_attributes': 1, "sk_timeStamp": 1}))
            proforma_invoice = []
            for data in result:
             proforma_invoice = data['all_attributes'].get('proforma_invoice',{})
            formatted_data={}
            if proforma_invoice:
             proforma_documents_list = []
 
             for doc_key, doc_value in proforma_invoice['proforma_documents'].items():
                proforma_documents_list.append({
                "doc_name": doc_value["doc_name"],
                "doc_body": doc_value["doc_body"]
                })
             commentsList = []  
             if 'comments' in proforma_invoice:
                    comments = proforma_invoice['comments']
                    commentCount = sum(
                            1 for c in comments.values() if isinstance(c, dict) and c.get('comment')
                    )
                    for comment_key, comment_value in comments.items():
                        if isinstance(comment_value, dict):
                            commentsList.append(comment_value)
                            count = sum(len(comment.get('attachments', [])) for comment in commentsList if isinstance(comment, dict))
             else:
                 commentCount = 0
                 count = 0
             
             created_time_str = proforma_invoice.get('created_time', '')
             if created_time_str:
                created_time = datetime.fromisoformat(created_time_str).strftime("%d/%m/%Y, %I:%M %p")
             else:
                created_time = ""
 
             comments = proforma_invoice.get('comments', {})
             for key, comment in comments.items():
                 created_time_str = comment.get('created_time')
                 if created_time_str:
                  try:
                    formatted_time = datetime.fromisoformat(created_time_str).strftime("%d/%m/%Y, %I:%M %p")
                    comment['created_time'] = formatted_time  # Update the created_time
                  except ValueError:
                     comment['created_time'] = "Invalid date format"
               
             formatted_data = {
                "no_of_items": proforma_invoice['no_of_items'],
                "payment_date": datetime.strptime(proforma_invoice['payment_date'], "%Y-%m-%d").strftime("%d-%m-%Y"),
                "pi_number": proforma_invoice['pi_number'],
                "documents": proforma_documents_list,
                "status": proforma_invoice.get('status',""),
                "tax_amount": proforma_invoice['tax_amount'],
                "comments":proforma_invoice.get('comments', {}),
                "comments_count": commentCount,
                "attachment_count":count,
                "createdTime":created_time,
             }
            return {'statusCode': 200, 'body': formatted_data}
           
 
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
   
 


     
    # def cmsPOGetProformaInvoice(request_body):
    #     try:
    #         data = request_body #test
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         po_id = data['po_id']
    #         result = list(db_con.NewPurchaseOrder.find({'all_attributes.po_id': po_id, "gsipk_table": "PurchaseOrder"},{'_id': 0, 'all_attributes': 1, "sk_timeStamp": 1}))
    #         proforma_invoice = []
    #         for data in result:
    #          proforma_invoice = data['all_attributes'].get('proforma_invoice',{})
    #         formatted_data={}
    #         if proforma_invoice:
    #          proforma_documents_list = []

    #          for doc_key, doc_value in proforma_invoice['proforma_documents'].items():
    #             proforma_documents_list.append({
    #             "doc_name": doc_value["doc_name"],
    #             "doc_body": doc_value["doc_body"]
    #             })
    #          commentsList = []   
    #          if 'comments' in proforma_invoice:
    #                 comments = proforma_invoice['comments']
    #                 commentCount = sum(
    #                         1 for c in comments.values() if isinstance(c, dict) and c.get('comment')
    #                 )
    #                 for comment_key, comment_value in comments.items():
    #                     if isinstance(comment_value, dict):
    #                         commentsList.append(comment_value)
    #                         count = sum(len(comment.get('attachments', [])) for comment in commentsList if isinstance(comment, dict)) 
    #          else:
    #              commentCount = 0
    #              count = 0
             
    #          created_time_str = proforma_invoice.get('created_time', '')
    #          if created_time_str:
    #             created_time = datetime.fromisoformat(created_time_str).strftime("%d/%m/%Y, %I:%M %p")
    #          else:
    #             created_time = ""   
    #          formatted_data = {
    #             "no_of_items": proforma_invoice['no_of_items'],
    #              "status": proforma_invoice['status'],
    #             "payment_date": proforma_invoice['payment_date'],
    #             "pi_number": proforma_invoice['pi_number'],
    #             "documents": proforma_documents_list,
    #             "tax_amount": proforma_invoice['tax_amount'],
    #             "comments":proforma_invoice.get('comments',{}),
    #             "comments_count": commentCount,
    #             "attachment_count":count,
    #             "createdTime":created_time
    #          }
    #         return {'statusCode': 200, 'body': formatted_data}
            

    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400, 'body': 'Bad Request(check data)'} 
    
    def CmsDraftEditProformaInvoice(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            
            # Generate the pk_id for the new Proforma Invoice
            pk_ids = list(db_con.ProformaInvoice.find({'pk_id': {'$regex': '^PI'}}, {'pk_id': 1}))
            if len(pk_ids) == 0: 
                pk_id = "PIPTG1"
                max_pk = 1
            else:
                pk_filter = [int(x['pk_id'][5:]) for x in pk_ids] 
                max_pk = max(pk_filter) + 1 
                pk_id = "PIPTG" + str(max_pk) 
 
            sk_timeStamp = (datetime.now()).isoformat()
            PI_no = data["pi_id"]
            pi_data = db_con.DraftProformaInvoice.find_one({"all_attributes.pi_id": PI_no})
            
            if not pi_data:
                return {'statusCode': 404, 'body': 'Proforma Invoice not found'}

            # Update fields based on the incoming data
            update_fields = {
                "ship_to": data.get("ship_to", pi_data["all_attributes"].get("ship_to", {})),
                "req_line": data.get("req_line", pi_data["all_attributes"].get("req_line", "")),
                "pi_terms_conditions": data.get("pi_terms_and_conditions", pi_data["all_attributes"].get("pi_terms_conditions", "")),
                "kind_attn": data.get("kind_attn", pi_data["all_attributes"].get("kind_attn", {})),
                "primary_document_details": data.get("primary_document_details", pi_data["all_attributes"].get("primary_document_details", {})),
                "products_list": data.get("products_list", pi_data["all_attributes"].get("products_list", {})),
                "total_amount": data.get("total_amount", pi_data["all_attributes"].get("total_amount", {})),
                "secondary_doc_details": data.get("secondary_doc_details", pi_data["all_attributes"].get("secondary_doc_details", {})),
                "client_id": data.get("client_id", pi_data["all_attributes"].get("client_id", "")),
                "pi_id": PI_no,
                #work from here...
            }
          
            # Generate the Proforma Invoice ID based on the current date
            pi_date = update_fields["primary_document_details"].get("Pi_date", "")
         
            if pi_date:
                try:
                    pi_date_obj = datetime.strptime(pi_date, "%Y-%m-%d")
                    pi_month = pi_date_obj.month
                    pi_year = pi_date_obj.strftime("%y")
                    pi_next = (pi_date_obj.year + 1) % 100
                    pi_id = f"EPL/PI/{max_pk}/{pi_month}/{pi_year}-{pi_next:02d}"
                    update_fields["pi_id"] = pi_id
                except ValueError:
                    return {'statusCode': 400, 'body': 'Invalid PI_Date format'}
            else:
                return {'statusCode': 400, 'body': 'PI_Date is required'}

            new_pi_data = {
                'pk_id': pk_id,
                'sk_timeStamp': sk_timeStamp,
                'all_attributes': update_fields,
                'gsisk_id': "open",
                'gsipk_table': "ProformaInvoice",
                'lsi_key': " "
            }
            
            
            db_con.all_tables.update_one({'pk_id': 'top_ids'}, {'$set': {'all_attributes.ProformaInvoice': pk_id}})
            db_con.ProformaInvoice.insert_one(new_pi_data)
            db_con.DraftProformaInvoice.delete_one({'all_attributes.pi_id': PI_no})
            

            # Delete the old draft Proforma Invoice
            

            conct.close_connection(client)
            return {'statusCode': 200, 'body': 'Proforma Invoice updated successfully'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'internal server error'}


    
    # def CmsDraftEditProformaInvoice(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         client = db_conct['client']
    #         PI_no=data["pi_id"]
    #         sk_timeStamp = (datetime.now()).isoformat()
    #         pi_data = db_con.DraftProformaInvoice.find_one({"all_attributes.pi_id": PI_no})
    #         print(pi_data)
    #         if not pi_data:
    #             return {'statusCode': 404, 'body': 'Service Order not found'}

    #         update_fields = {
    #             "ship_to": data.get("ship_to", pi_data["all_attributes"].get("ship_to", {})),
    #             "req_line": data.get("req_line", pi_data["all_attributes"].get("req_line", "")),
    #             "pi_terms_conditions": data.get("pi_terms_and_conditions", pi_data["all_attributes"].get("pi_terms_conditions", "")),
    #             "kind_attn": data.get("kind_attn", pi_data["all_attributes"].get("kind_attn", {})),
    #             "primary_document_details": data.get("primary_document_details", pi_data["all_attributes"].get("primary_document_details", {})),
    #             "products_list": data.get("products_list", pi_data["all_attributes"].get("products_list", {})),
    #             "total_amount": data.get("total_amount", pi_data["all_attributes"].get("total_amount", {})),
    #             "secondary_doc_details": data.get("secondary_doc_details", pi_data["all_attributes"].get("secondary_doc_details", {})),
    #             "client_id": data.get("client_id", pi_data["all_attributes"].get("client_id", "")),
    #             "pi_id": PI_no 
    #         }

    #         # Update the service order in the database
    #         db_con.DraftProformaInvoice.update_one(
    #             {"all_attributes.pi_id": PI_no},
    #             {"$set": {
    #                 "sk_timeStamp": sk_timeStamp,
    #                 "all_attributes": update_fields
    #             }}
    #         )

    #         conct.close_connection(client)
    #         return {'statusCode': 200, 'body': 'Proforma Invoice updated successfully'}

    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400, 'body': 'internal server error'}  





# import json
# from datetime import datetime, timedelta,date
# import base64
# from db_connection import db_connection_manage
# import sys
# import os
# from dateutil.relativedelta import relativedelta
# from bson import ObjectId
# from cms_utils import file_uploads

# conct = db_connection_manage()
# fiup = file_uploads()
# class ProformaInvoice():
#     def cmsproformaInvoicegetClientDetails(request_body):
#         try:
#             data = request_body
#             print(request_body)
#             env_type = data['env_type']
#             db_conct = conct.get_conn(env_type)
#             db_con = db_conct['db']
#             client = db_conct['client']
#             client_id=data['client_id']
#             sk_timeStamp = (datetime.now()).isoformat()
#             client_db = list(db_con.Clients.find({"all_attributes.client_id":client_id},{'_id':0}))
#             if client_db:
#                 result=[{"client_id":i['all_attributes']['client_id'],
#                     "client_name":i['all_attributes']['client_name'],
#                     "kind_attn":{"gst_number":"",
#                                     "pan_number":"",
#                                     "contact_number":i['all_attributes']['contact_number'],
#                                     "contact_details":i['all_attributes']['contact_number'],
#                                     "adress":""},
#                     "ship_to":{"gst_number":"",
#                                     "pan_number":"",
#                                     "contact_number":i['all_attributes']['contact_number'],
#                                     "contact_details":i['all_attributes']['contact_number'],
#                                     "adress":""},
#                     "request_line":"Dear Sir/Ma'am,We acknowledge with thanks in receipt of your above-mentioned purchase order for supplyof below parts In this connection we are pleased to submit our Proforma Invoice as below"
#                                     } for i in client_db]
#             else:
#                 return {'statusCode':404,'body':"No data for this client"}
#             return {'statusCode':200,'body':result[0]}
#         except Exception as err:
#             exc_type, exc_obj, tb = sys.exc_info()
#             f_name = tb.tb_frame.f_code.co_filename
#             line_no = tb.tb_lineno
#             #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
#             return {'statusCode': 400,'body': 'Internal server error'}
#     def cmsproformaInvoiceCreate(request_body):
#         try:
#             data = request_body
#             env_type = data['env_type']
#             db_conct = conct.get_conn(env_type)
#             db_con = db_conct['db']
#             client = db_conct['client']
#             client_id=data['client_id']
#             primary_doc_details = data.get("primary_document_details", {})
#             PI_date = primary_doc_details.get("PI_date", "")
#             sk_timeStamp = (datetime.now()).isoformat()

#             if PI_date:
#                 try:
#                     pi_date_obj = datetime.strptime(PI_date, "%Y-%m-%d")
#                     pi_month_year = f"{pi_date_obj.strftime('%m/%y')}-{(pi_date_obj.year + 1) % 100:02}"
#                     print(pi_month_year)

#                 except ValueError:
#                     return {'statusCode': 400, 'body': 'Invalid SO_Date format'}
#             else:
#                 return {'statusCode': 400, 'body': 'PI_Date is required'}
            
#             proforma_data = list(db_con.ProformaInvoice.find({}))
#             proforma_order_id = "1"
#             client_PI_num = f"EPL/PI/1/{pi_month_year}"

#             if proforma_data:
#                 update_id = list(db_con.all_tables.find({"pk_id": "top_ids"},{"_id":0}))
#                 if "ProformaInvoice" in update_id[0]['all_attributes']:
#                     update_id = (update_id[0]['all_attributes']['ProformaInvoice'][5:])
#                 else:
#                     update_id = "1"
#                 proforma_order_id = str(int(update_id) + 1)
#             PI_id = [i["all_attributes"]["PI_id"] for i in proforma_data if "PI_id" in i["all_attributes"]]
#             if PI_id:
#                 client_PI_num = f"EPL/PI/{proforma_order_id}/{pi_month_year}"

#             Proforma_data = {
#                 "pk_id": "PIPTG" + proforma_order_id,
#                 "sk_timeStamp": sk_timeStamp,
#                 "all_attributes": {
#                     "ship_to": data.get("ship_to", {}),
#                     "req_line": data.get("req_line", ""),
#                     "pi_terms_conditions": data.get("pi_terms_and_conditions", ""),
#                     "kind_attn": data.get("kind_attn", {}),
#                     "primary_document_details": primary_doc_details,
#                     "products_list": data.get("product_list", {}),
#                     "total_amount": data.get("total_amount", {}),
#                     "secondary_doc_details": data.get("secondary_doc_details", {}),
#                     "PI_id": client_PI_num,
#                     "client_id": data.get("client_id", ""),
#                 },
#                 "gsisk_id": "open",
#                 "gsipk_table": "ProformaInvoice",
#                 "lsi_key": ""
                
#             }

#             db_con.ProformaInvoice.insert_one(Proforma_data)
#             key = {'pk_id': "top_ids", 'sk_timeStamp': "123"}        
#             update_data = {
#                 '$set': {
#                     'all_attributes.ProformaInvoice': "PIPTG" + proforma_order_id
#                 }
#             }
#             db_con.all_tables.update_one(key, update_data)
#             conct.close_connection(client)
#             return {'statusCode': 200, 'body': 'New PI created successfully'}



#         except Exception as err:
#             exc_type, exc_obj, tb = sys.exc_info()
#             f_name = tb.tb_frame.f_code.co_filename
#             line_no = tb.tb_lineno
#             print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
#             return {'statusCode': 400,'body': 'Internal server error'}
#     def CmsEditProformaInvoice(request_body):
#         try:
#             data = request_body
#             env_type = data['env_type']
#             db_conct = conct.get_conn(env_type)
#             db_con = db_conct['db']
#             client = db_conct['client']
#             PI_no=data["PI_id"]
#             sk_timeStamp = (datetime.now()).isoformat()
#             pi_data = db_con.ProformaInvoice.find_one({"all_attributes.PI_id": PI_no})
#             if not pi_data:
#                 return {'statusCode': 404, 'body': 'Service Order not found'}

#             update_fields = {
#                 "ship_to": data.get("ship_to", pi_data["all_attributes"].get("ship_to", {})),
#                 "req_line": data.get("req_line", pi_data["all_attributes"].get("req_line", "")),
#                 "pi_terms_and_conditions": data.get("pi_terms_and_conditions", pi_data["all_attributes"].get("pi_terms_and_conditions", "")),
#                 "kind_attn": data.get("kind_attn", pi_data["all_attributes"].get("kind_attn", {})),
#                 "primary_document_details": data.get("primary_document_details", pi_data["all_attributes"].get("primary_document_details", {})),
#                 "products_list": data.get("products_list", pi_data["all_attributes"].get("products_list", {})),
#                 "total_amount": data.get("total_amount", pi_data["all_attributes"].get("total_amount", {})),
#                 "secondary_doc_details": data.get("secondary_doc_details", pi_data["all_attributes"].get("secondary_doc_details", {})),
#                 "client_id": data.get("client_id", pi_data["all_attributes"].get("client_id", "")),
#                 "pi_id": PI_no 
#             }

#             # Update the service order in the database
#             db_con.ProformaInvoice.update_one(
#                 {"all_attributes.PI_id": PI_no},
#                 {"$set": {
#                     "sk_timeStamp": sk_timeStamp,
#                     "all_attributes": update_fields
#                 }}
#             )

#             conct.close_connection(client)
#             return {'statusCode': 200, 'body': 'Proforma Invoice updated successfully'}

#         except Exception as err:
#             exc_type, exc_obj, tb = sys.exc_info()
#             f_name = tb.tb_frame.f_code.co_filename
#             line_no = tb.tb_lineno
#             print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
#             return {'statusCode': 400, 'body': 'internal server error'}  

#     def proformaInvoiceEditGet(request_body):
#         try:
#             data = request_body
#             print(request_body)
#             env_type = data['env_type']
#             db_conct = conct.get_conn(env_type)
#             db_con = db_conct['db']
#             client = db_conct['client']
#             pi_id = data['PI_id']
#             proforma_record = db_con.ProformaInvoice.find_one({"all_attributes.PI_id":pi_id},{"_id":0,"all_attributes":1})
#             print(proforma_record)
#             if proforma_record:
#                 return {'statusCode': 200,'body': proforma_record['all_attributes']}
#             else:
#                 return {'statusCode': 400,'body': 'No record found for this pi id'}
#         except Exception as err:
#             exc_type, exc_obj, tb = sys.exc_info()
#             f_name = tb.tb_frame.f_code.co_filename
#             line_no = tb.tb_lineno
#             #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
#             return {'statusCode': 400,'body': 'Internal server error'}