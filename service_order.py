
import json
from datetime import datetime, timedelta,date
import base64
from db_connection import db_connection_manage
import sys
import os, re
from dateutil.relativedelta import relativedelta
from bson import ObjectId
from cms_utils import file_uploads

conct = db_connection_manage()


def file_get(path):
    if path:
        return path
    else:
        return ""

class ServiceOrder():
    
    def CmsNewServiceOrderCreate(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sk_timeStamp = (datetime.now()).isoformat()
            
            primary_doc_details = data.get("primary_document_details", {})
            so_date = primary_doc_details.get("so_date", "")
            
            # Extract month and year from SO_Date
            if so_date:
                try:

                    so_date_obj = datetime.strptime(so_date, "%Y-%m-%d")
                    po_month = so_date_obj.strftime("%m")
                    po_year = so_date_obj.strftime("%y")
                    next_month_obj = so_date_obj + relativedelta(months=1)
                    next_month = next_month_obj.strftime("%m")
                    next_year = next_month_obj.strftime("%y")

                    next_year = str(int(po_year) + 1).zfill(2)
                    so_month_year = f"{po_month}/{po_year}-{next_year}"

                    # so_date_obj = datetime.strptime(so_date, "%Y-%m-%d")
                    # so_month_year = so_date_obj.strftime("%m-%y")
                except ValueError:
                    return {'statusCode': 400, 'body': 'Invalid SO_Date format'}
            else:
                return {'statusCode': 400, 'body': 'SO_Date is required'}
            
            service_orders = list(db_con.NewPurchaseOrder.find({}))
            service_order_id = "1"
            client_so_num = f"EPL/SO/1/{so_month_year}"

            if service_orders:
                # update_id = list(db_con.all_tables.find({"pk_id": "top_ids"}))
                update_id = list(db_con.all_tables.find({"pk_id": "top_ids"}))
                print(update_id)
                if "ServiceOrder" in update_id[0]['all_attributes']:
                    update_id = (update_id[0]['all_attributes']['ServiceOrder'][5:])
                    print(update_id)
                else:
                    update_id = "1"
                service_order_id = str(int(update_id) + 1)
                print(service_order_id)
            last_client_so_nums = [i["all_attributes"]["so_id"] for i in service_orders if "so_id" in i["all_attributes"]]
            if last_client_so_nums:
                client_so_num = f"EPL/SO/{service_order_id}/{so_month_year}"

            service_order_data = {
                "pk_id": "SOPTG" + service_order_id,
                "sk_timeStamp": sk_timeStamp,
                "all_attributes": {
                    "ship_to": data.get("ship_to", {}),
                    "req_line": data.get("req_line", ""),
                    "so_terms_conditions": data.get("so_terms_conditions", ""),
                    "kind_attn": data.get("kind_attn", {}),
                    "primary_document_details": primary_doc_details,
                    "job_work_table": data.get("job_work_table", {}),
                    "total_amount": data.get("total_amount", {}),
                    "secondary_doc_details": data.get("secondary_doc_details", {}),
                    "so_id": client_so_num,
                    "partner_id": data.get("partner_id", ""),
                },
                "gsisk_id": "open",
                "gsipk_table": "ServiceOrder",
                "lsi_key": "Pending"
                
            }

            db_con.NewPurchaseOrder.insert_one(service_order_data)
            key = {'pk_id': "top_ids", 'sk_timeStamp': "123"}        
            update_data = {
                '$set': {
                    'all_attributes.ServiceOrder': "SOPTG" + service_order_id
                }
            }
            db_con.all_tables.update_one(key, update_data)
            conct.close_connection(client)
            return {'statusCode': 200, 'body': 'New SO created successfully'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'SO creation failed'}



    
    def CmsPurchaseOrderGetPartnersDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            partner_id = data.get("partner_id")

            match_stage = {
                "$match": {
                    "gsipk_table": "Partners",
                    "pk_id": partner_id
                }
            }
            # Execute aggregation pipeline
            pipeline = [
                match_stage
            ]
            vendors = list(db_con.Partners.aggregate(pipeline))
            # print(vendors)
            lst = sorted([
                {
                    'partner_id': item.get('pk_id', ""),
                    'partner_type': item.get('gsipk_id',""),
                    "ship_to": {
                        "company_name": "People Tech IT Consultancy Pvt Ltd",
                        "gst_number": "36AAGCP2263H2ZE",
                        "pan_number": " AAGCP2263H",
                        "contact_details": "Sudheendra Soma",
                        "contact_number": "9885900909",
                        "address": "Plot No.14 & 15, RMZ Futura Building, Block B, Hitech City, Hyderabad,Telangana, India- 500081"
                    },
                    "kind_Attn": {
                        "company_name":item["all_attributes"].get('partner_name', ""),
                        "gst_number": item["all_attributes"].get('gst_number', ""),
                        "pan_number": item["all_attributes"].get('pan_number', ""),
                        "contact_number": item["all_attributes"].get('contact_number', ""),
                        "address": item["all_attributes"].get('address1', "")
                    },
                    "req_line": """Dear Sir/Ma'am,
                                Please Supply the Items mentioned in Order subject to delivery, mode and other terms and conditions below and overleaf. Please confirm the acceptance of this order. If you expect any delay in supply,communicate the same immediately on receipt of this purchase order."""
                }
                for item in vendors
            ], key=lambda x: int(x['partner_id'].replace("PTGPAR", "")), reverse=False)
            # # #print(lst)
            conct.close_connection(client)
            return {'statusCode': 200, 'body': lst[0]}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'vendor deletion failed'}
        


    
    # def CmsGetPartnerNameList(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         client = db_conct['client']

    #         # Find partners and project partner_id and partner_name fields
    #         result = list(db_con.Partners.find({}, {"_id": 0, "all_attributes.partner_id": 1, "all_attributes.partner_name": 1}))

    #         # Extract partner_id and partner_name into the desired structure
    #         partner_list = [{"partnerId": partner['all_attributes']['partner_id'], "partnerName": partner['all_attributes']['partner_name']} for partner in result]

    #         conct.close_connection(client)

    #         # Convert list of partners to a dictionary
    #         partner_dict = {f"partner{index + 1}": partner for index, partner in enumerate(partner_list)}

    #         return {'statusCode': 200, 'body': partner_dict}

    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400, 'body': 'Failed to fetch partner names'}


    def CmsGetPartnerNameList(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']

            # Find partners and project partner_id and partner_name fields
            result = list(db_con.Partners.find({}, {"_id": 0, "all_attributes.partner_id": 1, "all_attributes.partner_name": 1}))

            # Extract partner_id and partner_name into the desired structure
            partner_list = [{"partnerId": partner['all_attributes']['partner_id'], "partnerName": partner['all_attributes']['partner_name']} for partner in result]

            conct.close_connection(client)
            return {'statusCode': 200, 'body': partner_list}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Failed to fetch partner names'}
        


    
    # def CmsUpdateServiceOrder(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         so_id = data['so_id']
            
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         client = db_conct['client']
    #         sk_timeStamp = (datetime.now()).isoformat()

    #         # Find the existing service order based on so_id
    #         existing_so = db_con.PurchaseOrder.find_one({"all_attributes.so_id": so_id})
    #         if not existing_so:
    #             return {'statusCode': 404, 'body': 'Service Order not found'}
            
    #         # Prepare update fields
    #         update_fields = {
    #             "ship_to": data.get("ship_to", existing_so["all_attributes"].get("ship_to", {})),
    #             "req_line": data.get("req_line", existing_so["all_attributes"].get("req_line", "")),
    #             "so_terms_conditions": data.get("so_terms_conditions", existing_so["all_attributes"].get("so_terms_conditions", "")),
    #             "kind_attn": data.get("kind_attn", existing_so["all_attributes"].get("kind_attn", {})),
    #             "primary_document_details": data.get("primary_document_details", existing_so["all_attributes"].get("primary_document_details", {})),
    #             "job_work_table": data.get("job_work_table", existing_so["all_attributes"].get("job_work_table", {})),
    #             "total_amount": data.get("total_amount", existing_so["all_attributes"].get("total_amount", {})),
    #             "secondary_doc_details": data.get("secondary_doc_details", existing_so["all_attributes"].get("secondary_doc_details", {})),
    #             "partner_id": data.get("partner_id", existing_so["all_attributes"].get("partner_id", "")),
    #             "so_id": so_id 
    #         }

    #         # Update the service order in the database
    #         db_con.PurchaseOrder.update_one(
    #             {"all_attributes.so_id": so_id},
    #             {"$set": {
    #                 "sk_timeStamp": sk_timeStamp,
    #                 "all_attributes": update_fields
    #             }}
    #         )

    #         conct.close_connection(client)
    #         return {'statusCode': 200, 'body': 'Service Order updated successfully'}

    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400, 'body': 'Service Order update failed'}

    def CmsUpdateServiceOrder(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            so_id = data['so_id']
            updatestatus = data["updatestatus"]
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sk_timeStamp = (datetime.now()).isoformat()

            # Find the existing service order based on so_id
            existing_so = db_con.NewPurchaseOrder.find_one({"all_attributes.so_id": so_id, "lsi_key": updatestatus})
            if not existing_so:
                return {'statusCode': 404, 'body': 'Service Order not found'}
            
            new_status = "Pending" if updatestatus == "Rejected" else updatestatus
            
            # Prepare update fields
            update_fields = {
                "ship_to": data.get("ship_to", existing_so["all_attributes"].get("ship_to", {})),
                "req_line": data.get("req_line", existing_so["all_attributes"].get("req_line", "")),
                "so_terms_conditions": data.get("so_terms_conditions", existing_so["all_attributes"].get("so_terms_conditions", "")),
                "kind_attn": data.get("kind_attn", existing_so["all_attributes"].get("kind_attn", {})),
                "primary_document_details": data.get("primary_document_details", existing_so["all_attributes"].get("primary_document_details", {})),
                "job_work_table": data.get("job_work_table", existing_so["all_attributes"].get("job_work_table", {})),
                "total_amount": data.get("total_amount", existing_so["all_attributes"].get("total_amount", {})),
                "secondary_doc_details": data.get("secondary_doc_details", existing_so["all_attributes"].get("secondary_doc_details", {})),
                "partner_id": data.get("partner_id", existing_so["all_attributes"].get("partner_id", "")),
                "so_id": so_id 
            }

            # Update the service order in the databased
            db_con.NewPurchaseOrder.update_one(
                {"all_attributes.so_id": so_id},
                {"$set": {
                    "sk_timeStamp": sk_timeStamp,
                    "all_attributes": update_fields,
                    "lsi_key": new_status
                }}
            )

            conct.close_connection(client)
            return {'statusCode': 200, 'body': 'Service Order updated successfully'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Service Order update failed'}
        

    # def CmsServiceOrderGet(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']       
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         client = db_conct['client']
    #         sk_timeStamp = (datetime.now()).isoformat()
    #         so_id = data['so_id']
    #         document = db_con.NewPurchaseOrder.find_one({'all_attributes.so_id': so_id}, {'_id': 0})
    #         if not document:
    #             return {'statusCode': 404, 'body': 'No purchase order found for the given status and po_id'}
    #         all_attributes = document["all_attributes"]
    #         primary_document_details = all_attributes.get("primary_document_details", {})
                        
    #         job_work_table = all_attributes.get("job_work_table", {})
    #         if not job_work_table:
    #             return {'statusCode': 400, 'body': 'job work table is missing or invalid'}           
            
    #         extracted_data = {
                
    #             "ship_to": all_attributes.get("ship_to", {}),
    #             "req_line": all_attributes.get("req_line", ""),
    #             "kind_attn": all_attributes.get("kind_attn", {}), 
    #             "primary_document_details": primary_document_details,
    #             # "client_po": all_attributes.get("client_po", {}),
    #             "total_amount": all_attributes.get("total_amount", {}),
    #             "secondary_doc_details": all_attributes.get("secondary_doc_details", {}),
    #             "job_work_table": job_work_table,
    #             "so_terms_conditions": all_attributes.get("so_terms_conditions", ""),
    #             "so_id": all_attributes.get("so_id", ""),
    #             "partner_id": all_attributes.get("partner_id", "")
                
    #         }

    #         return {'statusCode': 200, 'body': extracted_data}
        

    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400, 'body': 'Service Order get failed'}
    def CmsServiceOrderGet(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sk_timeStamp = (datetime.now()).isoformat()
            so_id = data['so_id']
            id = so_id.split('/')[1]
            if id.startswith('D'):
                document = db_con.DraftServiceOrder.find_one({'all_attributes.so_id': so_id}, {'_id': 0})
                if not document:
                    return {'statusCode': 404, 'body': 'No service order found for the given so_id'}
                all_attributes = document["all_attributes"]
                # primary_document_details = all_attributes.get("primary_document_details", {})
                # job_work_table = all_attributes.get("job_work_table", {})
                # if not job_work_table:
                #     return {'statusCode': 400, 'body': 'job work table is missing or invalid'}
                extracted_data = {
                    "ship_to": all_attributes.get("ship_to", {}),
                    "req_line": all_attributes.get("req_line", ""),
                    "kind_attn": all_attributes.get("kind_attn", {}),
                    # "primary_document_details": primary_document_details,
                    "primary_document_details": all_attributes.get("primary_document_details", {}),
                    # "client_po": all_attributes.get("client_po", {}),
                    "total_amount": all_attributes.get("total_amount", {}),
                    "secondary_doc_details": all_attributes.get("secondary_doc_details", {}),
                    # "job_work_table": job_work_table,
                    "job_work_table": all_attributes.get("job_work_table", {}),
                    "so_terms_conditions": all_attributes.get("so_terms_conditions", ""),
                    "so_id": all_attributes.get("so_id", ""),
                    "partner_id": all_attributes.get("partner_id", "")
                }
                return {'statusCode': 200, 'body': extracted_data}
            else:
                document = db_con.NewPurchaseOrder.find_one({'all_attributes.so_id': so_id}, {'_id': 0})
                if not document:
                    return {'statusCode': 404, 'body': 'No purchase order found for the given status and po_id'}
                all_attributes = document["all_attributes"]
                # primary_document_details = all_attributes.get("primary_document_details", {})
                # job_work_table = all_attributes.get("job_work_table", {})
                # if not job_work_table:
                #     return {'statusCode': 400, 'body': 'job work table is missing or invalid'}
                extracted_data = {
                    "ship_to": all_attributes.get("ship_to", {}),
                    "req_line": all_attributes.get("req_line", ""),
                    "kind_attn": all_attributes.get("kind_attn", {}),
                    # "primary_document_details": primary_document_details,
                    "primary_document_details": all_attributes.get("primary_document_details", {}),
                    # "client_po": all_attributes.get("client_po", {}),
                    "total_amount": all_attributes.get("total_amount", {}),
                    "secondary_doc_details": all_attributes.get("secondary_doc_details", {}),
                    # "job_work_table": job_work_table,
                    "job_work_table": all_attributes.get("job_work_table", {}),
                    "so_terms_conditions": all_attributes.get("so_terms_conditions", ""),
                    "so_id": all_attributes.get("so_id", ""),
                    "partner_id": all_attributes.get("partner_id", "")
                }
                return {'statusCode': 200, 'body': extracted_data}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Service Order get failed'}
        
    def CmsDraftServiceOrderCreate(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sk_timeStamp = (datetime.now()).isoformat()
            
            primary_doc_details = data.get("primary_document_details", {})
            so_date = primary_doc_details.get("so_date", "")
            
            # Extract month and year from SO_Date
            if so_date:
                try:

                    so_date_obj = datetime.strptime(so_date, "%Y-%m-%d")
                    po_month = so_date_obj.strftime("%m")
                    po_year = so_date_obj.strftime("%y")
                    next_month_obj = so_date_obj + relativedelta(months=1)
                    next_month = next_month_obj.strftime("%m")
                    next_year = next_month_obj.strftime("%y")

                    next_year = str(int(po_year) + 1).zfill(2)
                    so_month_year = f"{po_month}/{po_year}-{next_year}"

                    # so_date_obj = datetime.strptime(so_date, "%Y-%m-%d")
                    # so_month_year = so_date_obj.strftime("%m-%y")
                except ValueError:
                    return {'statusCode': 400, 'body': 'Invalid Draft_SO_Date format'}
            else:
                return {'statusCode': 400, 'body': 'Draft_SO_Date is required'}
            
            service_orders = list(db_con.DraftServiceOrder.find({}))
            service_order_id = "1"
            client_so_num = f"EPL/DSO/1/{so_month_year}"

            if service_orders:
                # update_id = list(db_con.all_tables.find({"pk_id": "top_ids"}))
                update_id = list(db_con.all_tables.find({"pk_id": "top_ids"}))
                print(update_id)
                if "DraftServiceOrder" in update_id[0]['all_attributes']:
                    update_id = (update_id[0]['all_attributes']['DraftServiceOrder'][6:])
                    print(update_id)
                else:
                    update_id = "1"
                service_order_id = str(int(update_id) + 1)
                print(service_order_id)
            last_client_so_nums = [i["all_attributes"]["dso_id"] for i in service_orders if "dso_id" in i["all_attributes"]]
            if last_client_so_nums:
                client_so_num = f"EPL/DSO/{service_order_id}/{so_month_year}"

            service_order_data = {
                "pk_id": "DSOPTG" + service_order_id,
                "sk_timeStamp": sk_timeStamp,
                "all_attributes": {
                    "ship_to": data.get("ship_to", {}),
                    "req_line": data.get("req_line", ""),
                    "so_terms_conditions": data.get("so_terms_conditions", ""),
                    "kind_attn": data.get("kind_attn", {}),
                    "primary_document_details": primary_doc_details,
                    "job_work_table": data.get("job_work_table", {}),
                    "total_amount": data.get("total_amount", {}),
                    "secondary_doc_details": data.get("secondary_doc_details", {}),
                    # "so_id": client_so_num,
                    "so_id": f"EPL/DSO/{service_order_id}/{so_month_year}",
                    "partner_id": data.get("partner_id", ""),
                },
                "gsisk_id": "open",
                "gsipk_table": "DraftServiceOrder",
                "lsi_key": "Pending"
                
            }

            db_con.DraftServiceOrder.insert_one(service_order_data)
            key = {'pk_id': "top_ids", 'sk_timeStamp': "123"}        
            update_data = {
                '$set': {
                    'all_attributes.DraftServiceOrder': "DSOPTG" + service_order_id
                }
            }
            db_con.all_tables.update_one(key, update_data)
            conct.close_connection(client)
            return {'statusCode': 200, 'body': 'New Draft_SO created successfully'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'SO creation failed'}

    def cmsServiceUpdateDraft(request_data):
        try:
            data = request_data
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            pk_ids = list(db_con.NewPurchaseOrder.find({'pk_id': {'$regex': '^S'}}, {'pk_id': 1}))
            if len(pk_ids) == 0: 
                pk_id = "SOPTG1"
                max_pk = 1
            else:
                pk_filter = [int(x['pk_id'][5:]) for x in pk_ids]
                max_pk = max(pk_filter) + 1
                pk_id = "SOPTG" + str(max_pk)
            sk_timeStamp = (datetime.now()).isoformat()
            primary_document_details = data.get("primary_document_details", {})
            kind_attn = data.get("kind_attn", {})
            ship_to = data.get("ship_to", {})
            secondary_doc_details = data.get("secondary_doc_details", {})
            job_work_table = data.get("job_work_table", {})
            total_amount = data.get("total_amount", {})
            so_date = primary_document_details.get("so_date", "")
            if so_date:
                try:
                    so_date = datetime.strptime(so_date, "%Y-%m-%d")
                except ValueError:
                    return {'statusCode': 400, 'body': 'Invalid invoice_date format'}
            else:
                return {'statusCode': 400, 'body': 'invoice_date is required'}
            so_month = so_date.month
            so_year = so_date.strftime("%y")  
            so_next = (so_date.year + 1) % 100  
            so_id = f"EPL/SO/{max_pk}/{so_month}/{so_year}-{so_next:02d}"
            item = {
                'pk_id': pk_id,
                'sk_timeStamp': sk_timeStamp,
                'all_attributes': {
                    'ship_to': ship_to,
                    'req_line': data.get('req_line', ''),
                    'so_terms_conditions': data.get('so_terms_conditions', ''),
                    'kind_attn': kind_attn,
                    'primary_document_details': primary_document_details,
                    'job_work_table': job_work_table,
                    'total_amount': total_amount,
                    'secondary_doc_details': secondary_doc_details, 
                    'so_id': so_id,
                    'partner_id': data['partner_id']
                },
                'gsisk_id': "open",
                'gsipk_table': "ServiceOrder",
                'lsi_key': "Pending"
            }
            db_con.DraftServiceOrder.delete_one({'all_attributes.so_id': data['so_id']})
            resp = db_con.NewPurchaseOrder.insert_one(item)
            db_con.all_tables.update_one({'pk_id': 'top_ids'}, {'$set': {'all_attributes.ServiceOrder': pk_id}})
            return {'statusCode': 200, 'body':'Service draft updated successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Service draft update failed'}

    def purchaseOrderCancel(request_body):
            try:
                data = request_body
                env_type = data['env_type']
                pk_id = data['pos_id']
                db_conct = conct.get_conn(env_type)
                db_con = db_conct['db']
                client = db_conct['client']

                id = pk_id.split('/')[1]
                print('pkid', id, pk_id)
                if id == 'SO':
                    db_con.NewPurchaseOrder.update_one(
                    {"all_attributes.so_id": pk_id},
                    {"$set": {
                        "lsi_key": 'Cancel',
                        "sk_timeStamp": (datetime.now()).isoformat()
                    }}
                )
                if id == 'PO':
                    db_con.NewPurchaseOrder.update_one(
                    {"all_attributes.po_id": pk_id},
                    {"$set": {
                        "lsi_key": 'Cancel'
                    }}
                )
                if id == 'INV':
                    db_con.NewPurchaseOrder.update_one(
                    {"all_attributes.inv_id": pk_id},
                    {"$set": {
                        "lsi_key": 'Cancel',
                        "sk_timeStamp": (datetime.now()).isoformat()
                    }}
                )
                if id == 'PI':
                    db_con.ProformaInvoice.update_one(
                    {"all_attributes.pi_id": pk_id},
                    {"$set": {
                        "lsi_key": 'Cancel',
                        "sk_timeStamp": (datetime.now()).isoformat()
                    }}
                )
                if id == 'CFPO':
                    db_con.ForcastPurchaseOrder.update_one(
                    {"all_attributes.Client_Purchaseorder_num": pk_id},
                    {"$set": {
                        "lsi_key": 'Cancel',
                        "sk_timeStamp": (datetime.now()).isoformat()
                    }}
                )
                if id == 'FCPO':
                    db_con.ForcastPurchaseOrder.update_one(
                    {"all_attributes.Client_Purchaseorder_num": pk_id},
                    {"$set": {
                        "lsi_key": 'Cancel',
                        "sk_timeStamp": (datetime.now()).isoformat()
                    }}
                )
                if id == 'QUOTE':
                    db_con.Quotations.update_one(
                    {"all_attributes.quote_id": pk_id},
                    {"$set": {
                        "lsi_key": 'Cancel',
                        "sk_timeStamp": (datetime.now()).isoformat()
                    }}
                )
                conct.close_connection(client)
                return {'statusCode': 200, 'body': 'Order Cancelled successfully'}

            except Exception as err:
                exc_type, exc_obj, tb = sys.exc_info()
                f_name = tb.tb_frame.f_code.co_filename
                line_no = tb.tb_lineno
                print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
                return {'statusCode': 400, 'body': 'Order Cancellation failed'}
            
    def getAllCancelledOrders(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']

            cancelled_orders_Array = []
            all_attributes_array = []
            result = list(db_con.NewPurchaseOrder.find({},{'_id': 0})) + list(db_con.ProformaInvoice.find({},{'_id': 0})) + list(db_con.ForcastPurchaseOrder.find({},{'_id': 0})) + list(db_con.Quotations.find({},{'_id': 0}))
            for data in result:
                if data["lsi_key"] == 'Cancel':
                    cancelled_orders_Array.append(data)
            for cancelled_orders in cancelled_orders_Array:
                all_attributes_array.append(cancelled_orders['all_attributes'])
            conct.close_connection(client)
            return {'statusCode': 200, 'body': all_attributes_array}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Order Cancellation failed'}
                
    def getServiceOrderDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            result = db_con.NewPurchaseOrder.find_one({'all_attributes.so_id': data['so_id']}, {'_id': 0, 'all_attributes': 1})
            if result is not None:
                return {'statusCode': 200, 'body': result['all_attributes']}
            else:
                return {'statusCode': 400, 'body': 'No record found'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Fetching service order details failed'}    
        
    def cmsDeliveryChallanSOGet(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            pk_id = data['so_id']
            bom_list = list(db_con.BOM.find({}, {'_id':0}))
            boms = []
            for i in bom_list:
                boms.append(i.get("all_attributes", {}).get("bom_id", " "))
            result = list(db_con.NewPurchaseOrder.find({'all_attributes.so_id': pk_id}, {'_id':0}))
            response = {}
            for i in result:
                # Remove gst_number and pan_number from ship_to
                ship_to = i['all_attributes']['ship_to']
                if 'gst_number' in ship_to:
                    del ship_to['gst_number']
                if 'pan_number' in ship_to:
                    del ship_to['pan_number']
                    
                # Remove gst_number and pan_number from kind_attn
                kind_attn = i['all_attributes']['kind_attn']
                if 'gst_number' in kind_attn:
                    del kind_attn['gst_number']
                if 'pan_number' in kind_attn:
                    del kind_attn['pan_number']
                response['ship_to'] = ship_to
                response['kind_attn'] = kind_attn
                # response['ship_to'] = i['all_attributes']['ship_to']
                # response['kind_attn'] = i['all_attributes']['kind_attn']
                response['secondary_doc_details'] = i['all_attributes']['secondary_doc_details']
                response['bom_list'] = boms
                response['so_id'] = i['all_attributes']['so_id']
                response['req_line'] = "All the below materials are for testing purpose only, we certify that the particulars given below are true and correct"
                response['partner_id'] = i['all_attributes']['partner_id']
            return {'statusCode': 200, 'body': response}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
        
    def cmsDCBOMGetComponents(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            res = list(db_con.BOM.find({'all_attributes.bom_id': data['bom_id']}))
            result = {}

            if res:
                e_parts = res[0].get('all_attributes', {}).get('E_parts', {})
                m_parts = res[0].get('all_attributes', {}).get('M_parts', {})

                def get_e_ptg_stock(part):
                    inventory_res = db_con.Inventory.find_one({
                        '$or': [
                            {'all_attributes.mfr_prt_num': part.get('mfr_prt_num')},
                            {'all_attributes.ptg_prt_num': part.get('ptg_prt_num')}
                        ]
                    })
                    return inventory_res['all_attributes'].get('qty', '0') if inventory_res else '0'

                def get_m_ptg_stock(part):
                    inventory_res = db_con.Inventory.find_one({
                        '$or': [
                            {'all_attributes.mfr_prt_num': part.get('vic_part_number')},
                            {'all_attributes.ptg_prt_num': part.get('ptg_prt_num')}
                        ]
                    })
                    return inventory_res['all_attributes'].get('qty', '0') if inventory_res else '0'

                for part_key, part_value in e_parts.items():
                    part_value['ptg_stock'] = str(get_e_ptg_stock(part_value))

                for part_key, part_value in m_parts.items():
                    part_value['ptg_stock'] = str(get_m_ptg_stock(part_value))

                for part_key, part_value in m_parts.items():
                    if "vic_part_number" in part_value:
                        part_value["mfr_prt_num"] = part_value.pop("vic_part_number")
                result['e_parts'] = e_parts
                result['m_parts'] = m_parts

            return {'statusCode': 200, 'body': result}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
        
    def cmsDeliveryChallanSOSave(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sk_timeStamp = (datetime.now()).isoformat()
            m_part = data.get('M-Category_info', [])
            e_part = data.get('E-Category_info', [])
            vic_part_dict = {}
            mfr_prt_dict = {}

            for part in m_part:
                part.pop('s.no', None)
                mfr_prt_num = part['mfr_prt_num']
                qty_per_board = part.get('qty_per_board', 0)
                provided_qty = int(part.get('provided_qty', 0))
                ctgr_name = part['ctgr_name']
                if not str(qty_per_board).isdigit():
                    conct.close_connection(client)
                    return {'statusCode': 400,
                            'body': f"Invalid quantity '{qty_per_board}' for mfr_prt_num: {mfr_prt_num}. Please enter a valid number."}
                result = db_con.Inventory.find_one(
                    {
                        "all_attributes.mfr_prt_num": mfr_prt_num,
                        "all_attributes.ctgr_name": ctgr_name
                    },
                    {
                        "_id": 0,
                        "qty": "$all_attributes.qty",  # Assuming 'ptg_stock' is stored as 'qty'
                        "cmpt_id": "$all_attributes.cmpt_id",
                        "ctgr_id": "$all_attributes.ctgr_id",
                        "ctgr_name": "$all_attributes.ctgr_name",
                        "prdt_name": "$all_attributes.prdt_name",
                        "material": "$all_attributes.material",
                        "manufacturer": "$all_attributes.manufacturer"
                    }
                )
                if result:
                    ptg_stock = int(result.get('qty', 0))
                    if provided_qty <= ptg_stock:
                        new_qty = ptg_stock - provided_qty
                        db_con.Inventory.update_one(
                            {
                                "all_attributes.mfr_prt_num": mfr_prt_num,
                                "all_attributes.ctgr_name": ctgr_name
                            },
                            {"$set": {"all_attributes.qty": str(new_qty)}}
                        )
                        result['qty_per_board'] = qty_per_board
                        part.update(result)
                        vic_part_dict[mfr_prt_num] = part
                    else:
                        conct.close_connection(client)
                        return {'statusCode': 400,
                                'body': f"Insufficient stock for mfr_prt_num: {mfr_prt_num}. Available: {ptg_stock}, Required: {provided_qty}"}
                else:
                    return {'statusCode': 300,
                            'body': f"No matching record found for mfr_prt_num: {mfr_prt_num} and ctgr_name: {ctgr_name}"}

            for part in e_part:
                part.pop('s.no', None)
                mfr_prt_num = part['mfr_prt_num']
                qty_per_board = part.get('qty_per_board', 0)
                provided_qty = int(part.get('provided_qty', 0))
                ctgr_name = part['ctgr_name']
                
                if not str(qty_per_board).isdigit():
                    conct.close_connection(client)
                    return {'statusCode': 400,
                            'body': f"Invalid quantity '{qty_per_board}' for mfr_prt_num: {mfr_prt_num}. Please enter a valid number."}
                result = db_con.Inventory.find_one(
                    {
                        "all_attributes.mfr_prt_num": mfr_prt_num,
                        "all_attributes.ctgr_name": ctgr_name
                    },
                    {
                        "_id": 0,
                        "qty": "$all_attributes.qty",
                        "cmpt_id": "$all_attributes.cmpt_id",
                        "ctgr_id": "$all_attributes.ctgr_id",
                        "ctgr_name": "$all_attributes.ctgr_name",
                        "prdt_name": "$all_attributes.prdt_name",
                        "material": "$all_attributes.material",
                        "manufacturer": "$all_attributes.manufacturer"
                    }
                )
                if result:
                    ptg_stock = int(result.get('qty', 0))
                    if provided_qty <= ptg_stock:
                        new_qty = ptg_stock - provided_qty
                        db_con.Inventory.update_one(
                            {
                                "all_attributes.mfr_prt_num": mfr_prt_num,
                                "all_attributes.ctgr_name": ctgr_name
                            },
                            {"$set": {"all_attributes.qty": str(new_qty)}}
                        )
                        result['qty_per_board'] = qty_per_board
                        part.update(result)
                        mfr_prt_dict[mfr_prt_num] = part
                    else:
                        conct.close_connection(client)
                        return {'statusCode': 400,
                                'body': f"Insufficient stock for mfr_prt_num: {mfr_prt_num}. Available: {ptg_stock}, Required: {provided_qty}"}
                else:
                    return {'statusCode': 300,
                            'body': f"No matching record found for mfr_prt_num: {mfr_prt_num} and ctgr_name: {ctgr_name}"}

            m_parts = {f"part{inx + 1}": item for inx, item in enumerate(vic_part_dict.values())}
            e_parts = {f"part{inx + 1}": item for inx, item in enumerate(mfr_prt_dict.values())}
            # Handling DC Month-Year
            primary_doc_details = data.get("primary_document_details", {})
            challan_date = primary_doc_details.get("challan_date", "")
            if challan_date:
                try:
                    challan_date_obj = datetime.strptime(challan_date, "%Y-%m-%d")
                    po_month = challan_date_obj.strftime("%m")
                    po_year = challan_date_obj.strftime("%y")
                    next_month_obj = challan_date_obj + relativedelta(months=1)
                    next_year = str(int(po_year) + 1).zfill(2)
                    dc_month_year = f"{po_month}/{po_year}-{next_year}"
                except ValueError:
                    return {'statusCode': 400, 'body': 'Invalid DC_Date format'}
            else:
                return {'statusCode': 400, 'body': 'DC_Date is required'}

            delivery_challans = list(db_con.DeliveryChallan.find({}))
            if delivery_challans:
                # Extract the current delivery challan ID
                top_id_data = db_con.all_tables.find_one({"pk_id": "top_ids"})
                if top_id_data and "DeliveryChallan" in top_id_data['all_attributes']:
                    delivery_challan_id = top_id_data['all_attributes']['DeliveryChallan'][5:]
                    delivery_challan_id = str(int(delivery_challan_id) + 1)
                else:
                    delivery_challan_id = "1"
            else:
                delivery_challan_id = "1"

            client_so_num = f"EPL/DC/{delivery_challan_id}/{dc_month_year}"

            dc_data = {
                "pk_id": "DCPTG" + delivery_challan_id,
                "sk_timeStamp": sk_timeStamp,
                "all_attributes": {
                    "so_id": data.get('so_id', ''),
                    "ship_to": data.get("ship_to", {}),
                    "kind_attn": data.get("kind_attn", {}),
                    "dc_terms_conditions": data.get("dc_terms_conditions", " "),
                    "req_line": data.get("req_line", " "),
                    "primary_document_details": primary_doc_details,
                    "M_parts": m_parts,
                    "E_parts": e_parts,
                    "secondary_doc_details": data.get("secondary_doc_details", {}),
                    "dc_id": client_so_num,
                    "partner_id": data.get("partner_id", ""),
                },
                "gsisk_id": "open",
                "gsipk_table": "DeliveryChallan",
                "lsi_key": "Pending"
            }

            db_con.DeliveryChallan.insert_one(dc_data)

            # Update the `top_ids` collection for the next delivery challan ID
            key = {'pk_id': "top_ids"}
            update_data = {
                '$set': {
                    'all_attributes.DeliveryChallan': "DCPTG" + delivery_challan_id
                }
            }
            db_con.all_tables.update_one(key, update_data)
            
            activity = {}
            activity_id = (db_con.all_tables.find_one({"pk_id":"top_ids"},{"_id":0,"all_attributes.ActivityId":1}))
            activity_id = int(activity_id['all_attributes'].get('ActivityId','0')) + 1
            total_parts = []
            partner_data = db_con.Partners.find_one({"all_attributes.partner_id": data.get("partner_id", "")}, {"_id": 0, "all_attributes": 1})
            for part_key, part_data in m_parts.items():
                total_parts.append(part_data)
            for epart_key, epart_data in e_parts.items():
                total_parts.append(epart_data)
            for part_data in total_parts:
                print(part_data)
                activity[part_data.get("cmpt_id", "")] = {
                    "mfr_prt_num": part_data.get("mfr_prt_num", "-"),
                    "date": str(date.today()),
                    "action": "Utilized",
                    "Description": "Utilized",
                    "issued_to": partner_data['all_attributes']['partner_name'],
                    "dc_id": "DCPTG" + delivery_challan_id,
                    "invoice_no": data.get("invoice_num", ''),
                    "cmpt_id": part_data.get("cmpt_id", ""),
                    "ctgr_id": part_data.get("ctgr_id", ""),
                    "prdt_name": part_data.get("prdt_name", ""),
                    "description": part_data.get("description", ""),
                    "packaging": part_data.get("packaging", ""),
                    "closing_qty": int(part_data.get("ptg_stock", "")) - int(part_data.get("provided_qty", "")),
                    "qty": part_data.get("provided_qty", ""),
                    "batchId": part_data.get("batch_no", ""),
                    "used_qty": "0",
                    "lot_no": part_data.get("lot_id", "testlot")
                }
            db_con['ActivityDetails'].insert_one(
                    {
                        "pk_id": f"ACTID{activity_id}",
                        "sk_timeStamp": sk_timeStamp,
                        "all_attributes": activity,
                        "gsipk_table": "ActivityDetails",
                        "gsisk_id": "DCPTG" + delivery_challan_id,
                        "lsi_key": "Utilized"
                    }
                )
            
            conct.close_connection(client)
            return {'statusCode': 200, 'body': 'Delivery Challan created successfully'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
    def cmsGetServiceOrderCardDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            #client = db_conct['client']
            so_id=data['so_id']
            a={}
        
            result = list(db_con.NewPurchaseOrder.find({'all_attributes.so_id':so_id, "gsipk_table":"ServiceOrder"},{'_id':0,'all_attributes':1,"sk_timeStamp":1}))
            dates = [datetime.strptime(part['delivery_date'], "%Y-%m-%d") for part in result[0]['all_attributes'].values() if 'delivery_date' in part]
            if 'comments' in result[0]['all_attributes']:
                a['cmts_atcmts'] = result[0]['all_attributes']['comments']
                a['comment_count'] = sum(
                    1 for c in result[0]['all_attributes']['comments'].values() if isinstance(c, dict) and c.get('comment')
                )
                commentsList = []
                for i in result[0]['all_attributes']['comments']:
                    commentsList.append(result[0]['all_attributes']['comments'][i])
                a['attachments_count'] = sum(len(item['attachments']) for item in commentsList)
                
    
            else:
                a['comment_count'] = 0  
                a['attachments_count'] = 0
              
            if 'grand_total' in result[0]['all_attributes']['total_amount']:
                    a['grand_total'] = result[0]['all_attributes']['total_amount']['grand_total']
            else:
                  a['grand_total']= result[0]['all_attributes']['total_amount']['grandTotal']     
            
            a['parts_count'] = len(result[0]['all_attributes']['job_work_table'])
            delivery_date = result[0]['all_attributes']['job_work_table']['part1']['delivery_date']
            if delivery_date !="":
             dates = [datetime.strptime(part['delivery_date'], "%Y-%m-%d") 
             for part in result[0]['all_attributes']["job_work_table"].values() 
                    if 'delivery_date' in part]
             a['delivery_date'] = max(dates).strftime("%d/%m/%Y") if dates else None
            else:
                a['delivery_date']="" 

            sk_timestamp=result[0]['sk_timeStamp']
            formatted_date = datetime.strptime(sk_timestamp, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d,%I:%M %p")
            a['so_created_date'] = formatted_date  
            return {'statuscode':200,'body':a}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Comment adding failed'}    

    def cmsSaveCommentsForServiceOrder(request_body):

        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            so_id = data['so_id']
            #inward_id = data['inward_id']
            datasList = []
            attachment_dict = {}
            if "attachment" in data.keys() and data["attachment"]:
                for attachment in data['attachment']:
                    document_body = attachment['doc_body']
                    document_name = attachment['doc_name']
                    extra_type = ""
                    attachment_url = file_uploads.upload_file("ServiceOrder", "PtgCms" + env_type, extra_type, "ServiceOrder" + so_id, document_name, document_body)
                    attachment_dict = {
                                "doc_name": document_name,
                                "doc_body": attachment_url
                                }
                    datasList.append(attachment_dict) # Adds or updates the dictionary entry for each attachment
            else:
                attachment_dict = {}
                datasList = []

            comment_data = {
                "created_time": datetime.now().isoformat(),
                "comment": data["comment"],
                "attachments": datasList  # This dictionary now contains all attachments
            }
            gate_entry_result = db_con.NewPurchaseOrder.find_one({"all_attributes.so_id": so_id}, {"_id": 0, "all_attributes": 1})
            comments_dict = gate_entry_result.get('all_attributes', {}).get('comments', {})
            if not isinstance(comments_dict, dict):
                if isinstance(comments_dict, list):
                    comments_dict = {f"cmts_atcmts_{i + 1}": v for i, v in enumerate(comments_dict)}
                else:
                    comments_dict = {}
            new_comment_key = f"cmts_atcmts_{len(comments_dict) + 1}"
            # Add the new comment to the dictionary
            comments_dict[new_comment_key] = comment_data
            db_con.NewPurchaseOrder.update_one(
                    {"all_attributes.so_id": so_id},
                    {"$set": {"all_attributes.comments": comments_dict}}
            )
            return{'statusCode': 200, 'body': f'Comment added successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Comment adding failed'}  

    def cmsGetDCCards(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            res = list(db_con.DeliveryChallan.find({'all_attributes.so_id': data['so_id']}, {'_id': 0}))
            response_list = []
            for i in res:
                # Extracting dc_id
                dc_id = i['all_attributes'].get('dc_id', '')

                # Extracting M_parts and E_parts into a single list of items
                m_parts = i['all_attributes'].get('M_parts', {})
                e_parts = i['all_attributes'].get('E_parts', {})

                # Formatting the timestamp
                raw_time = i.get('sk_timeStamp', '')
                if raw_time:
                    # Convert the timestamp into datetime object
                    timestamp = datetime.strptime(raw_time, '%Y-%m-%dT%H:%M:%S.%f')
                    formatted_time = timestamp.strftime('%d/%m/%Y, %I:%M %p')
                else:
                    formatted_time = ''

                # Constructing the response structure
                response_list.append({
                    'dc_id': dc_id,
                    'items': len(m_parts) + len(e_parts),
                    'date_time': formatted_time
                })

            return {'statusCode': 200, 'body': response_list}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}  

    def getDCID(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            res = list(db_con.DeliveryChallan.find({'all_attributes.so_id': data['so_id']},{'_id':0}))
            datas = []
            for i in res:
                datas.append(i['all_attributes']['dc_id'])
            if datas:
                response = {
                    'statusCode':200,
                    "data":datas
                }
            else:
                response = {
                    "statusCode":404,
                    "message":"data not found"
                }    
            return response    

            

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}   
        
    def getDCDataByDCID(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            return_id = data.get("return_id", "")
            filter = {"all_attributes.dc_id": data["dc_id"]}
            projection = {
                "_id": 0
            }
            result = (db_con.DeliveryChallan.find_one(filter, projection))
            if result:
                bom_id = result['all_attributes'].get('bom_id','')
                category = db_con["Metadata"].find()
                category = {
                    item['pk_id'].replace("MDID", "CTID"): (
                        {
                            "ctgr_name": item['gsisk_id'],
                            "sub_categories": {
                                key: value for key, value in item['sub_categories'].items()
                            }
                        }
                        if item['gsipk_id'] == 'Electronic'
                        else {"ctgr_name": item['gsisk_id']}
                    )
                    for item in category
                }
                filter = {"gsipk_table": "Inventory"}
                projection = {
                    "_id": 0,
                    "all_attributes": 1
                }
                inventory = list(db_con["Inventory"].find(filter, projection))
                inventory = {item["all_attributes"]['cmpt_id']: item["all_attributes"] for item in inventory}
                part_info = {}
                if data.get('status', '') == "PurchaseReturn":
                    return_ref = {}
                    return_inventory = list(db_con.GateEntry.find({"all_attributes.return_id": return_id}))
                    for item in return_inventory:
                        for part_key, part_attributes in item["all_attributes"]['parts'].items():
                            cmpt_id = part_attributes['cmpt_id']
                            received_qty = part_attributes['received_qty']
                            return_ref[cmpt_id] = received_qty if cmpt_id not in return_ref else str(
                                int(return_ref[cmpt_id]) + int(received_qty))
                    return_data = db_con["PurchaseReturn"].find_one(
                        {"gsipk_table": "PurchaseReturn", "pk_id": return_id})
                    part_info["parts"] = {
                        key: {
                            "cmpt_id": value['cmpt_id'],
                            "ctgr_id": value['ctgr_id'],
                            "unit_price": value['price_per_piece'],
                            "received_qty": '0' if not return_ref and value[
                                'cmpt_id'] not in return_ref else return_ref.get(value['cmpt_id'], '0'),
                            "balance_qty": str(int(value['qty']) - abs(
                                int('0' if not return_ref and value['cmpt_id'] not in return_ref else return_ref[
                                    value['cmpt_id']]))),
                            "description": inventory[value['cmpt_id']].get('description', '-'),
                            "packaging": inventory[value['cmpt_id']].get('packaging', '-'),
                            "mfr_prt_num": value['mfr_prt_num'],
                            "manufacturer": inventory[value['cmpt_id']].get('manufacturer', '-'),
                            "price": value['price'],
                            "qty": value['qty'],
                            "delivery_date": value.get('delivery_date', ''),
                            "ctgr_name": category[value['ctgr_id']]['ctgr_name'],
                            "department": value['department'],
                            "prdt_name": category[value['ctgr_id']]['sub_categories'][
                                inventory[value['cmpt_id']]['sub_ctgr']]
                            if value['department'] != 'Mechanic' else inventory[value['cmpt_id']]['prdt_name']
                        }
                        for key, value in return_data['all_attributes']['parts'].items()
                        if str(int(value['qty']) - int(
                            '0' if not return_ref and value['cmpt_id'] not in return_ref else return_ref.get(
                                value['cmpt_id'], '0'))) != '0'
                    }
                    part_info["order_id"] = result["pk_id"]
                    part_info['return_id'] = data['return_id']
                    part_info["order_date"] = result["sk_timeStamp"][:10]
                    part_info['total_ordered_quantity'] = result['all_attributes']['total_qty']
                    return {"statusCode": 200, "body": part_info}
                else:
                    parts = {}
                    part_counter = 1
                    for key in ['M_parts', 'E_parts']:
                        for part_key, part_value in result['all_attributes'][key].items():
                            unique_key = f"part{part_counter}"
                            parts[unique_key] = part_value
                            part_counter += 1
                    result['all_attributes']['parts'] = parts
                    gate_entry = list(db_con["ServiceOrderGateEntry"].find({"gsipk_table": "ServiceOrderGateEntry", "all_attributes.dc_id": data["dc_id"]}))
                    ref = {}
                    for item in gate_entry:
                        for key, value in item['all_attributes']['parts'].items():
                            ref[value['cmpt_id']] = int(value['received_qty']) if value['cmpt_id'] not in ref else ref[value['cmpt_id']] + int(value['received_qty'])
                    part_info["parts"] = {
                        key: {
                            "cmpt_id": value['cmpt_id'],
                            "ctgr_id": value['ctgr_id'],
                            "received_qty": value.get('received_qty', '0'),
                            "description": inventory[value['cmpt_id']].get('description', ''),
                            "packaging": inventory[value['cmpt_id']].get('packaging', ''),
                            "mfr_prt_num": value['mfr_prt_num'],
                            "manufacturer": inventory[value['cmpt_id']].get('manufacturer', '-'),
                            "qty": value['qty'],
                            "delivery_date": value.get('delivery_date', ''),
                            "ctgr_name": category[value['ctgr_id']]['ctgr_name'],
                            "department": value['department'],
                            "prdt_name": category[value['ctgr_id']]['sub_categories'][
                                inventory[value['cmpt_id']]['sub_ctgr']]
                            if value['department'] != 'Mechanic' else inventory[value['cmpt_id']]['prdt_name'],
                            "provided_qty": str(int(value['provided_qty'])),
                            "balance_qty": str(
                                abs(int(value['provided_qty']) - abs(ref[value['cmpt_id']]) if gate_entry else int(
                                    value['provided_qty'])))
                        }
                        for key, value in result['all_attributes']['parts'].items()
                        if (not gate_entry or value['cmpt_id'] in ref) and (
                            str(int(value['qty']) - ref[value['cmpt_id']]) if gate_entry else value['qty']) != "0"
                    }
                    par_id=result['all_attributes']['partner_id']
                    query=list(db_con.Partners.find({'pk_id':par_id},{'_id':0,'all_attributes':1}))
                    part_info["order_id"] = result["pk_id"]
                    part_info["order_date"] = result["sk_timeStamp"][:10]
                    part_info['total_ordered_quantity'] = result['all_attributes'].get('total_qty', '-')
                    part_info['vendor_poc_name'] = query[0]['all_attributes'].get('vendor_poc_name', '-')
                    part_info['vendor_poc_contact_num'] = query[0]['all_attributes'].get('vendor_poc_contact_num', '-')
                    part_info['receiver'] = result['all_attributes']['ship_to'].get('company_name', '-')
                    part_info['receiver_contact'] = result['all_attributes']['ship_to'].get('phone_number', '-')
                    part_info['dc_id'] = result['all_attributes']['dc_id']
                    return {"statusCode": 200, "body": part_info}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Gate Entry Failed'}  

    def cmsCreateServiceOrderGateEntry(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            time = (datetime.now()).isoformat()
            dc_id = data['dc_id']
            sender_name = data['sender']
            sender_contact_number = data['sender_contact']
            receiver_name = data.get('receiver', '')
            receiver_contact = data.get('receiver_contact', '')
            parts = data['parts']
            so_tablename = "DeliveryChallan"
            purchase_order_query = {"gsipk_table": "DeliveryChallan", "all_attributes.dc_id": dc_id}
            result = db_con.DeliveryChallan.find_one({"gsipk_table": "DeliveryChallan", "all_attributes.dc_id": dc_id})
            if result:
                print("Came to result")
                inventory_query = {"gsipk_table": 'Inventory'}
                invent_data = list(db_con.Inventory.find(inventory_query))
                invent_data = {item['pk_id']: item for item in invent_data}
                qa_test_query = {"gsipk_table": 'ServiceOrderGateEntry', "gsisk_id": dc_id}
                qatest = list(db_con.ServiceOrderGateEntry.find(qa_test_query))
                qa_ref = {}
                for entry in qatest:
                    part = entry["all_attributes"]['parts']
                    for part_key, value in part.items():
                        qa_ref[value['cmpt_id']] = int(value['qty']) if value['cmpt_id'] not in qa_ref else qa_ref[
                                                                                                                value[
                                                                                                                    'cmpt_id']] + int(
                            value['qty'])
                vendorId = result['all_attributes']['partner_id']
                pending = 'QA_test'
                parts = (result)['all_attributes'].get("parts", {})
                part_ref = {part['cmpt_id']: int(part['qty']) for key, part in parts.items()}
                part_ref = {key: part_ref[key] + qa_ref.get(key, 0) for key in part_ref.keys()}
                parts = data.get("parts", {})
                if isinstance(parts, dict):
                    parts = list(parts.values())
                total_qty = sum(int(part.get('received_qty', 0)) for part in parts)
                if not total_qty:
                    return {"statusCode": 400, "body": "Cannot create record without components"}
                pk_query = {"all_attributes.dc_id": dc_id}
                projection = {"pk_id": 1, "_id": 0}
                pk_id_details = db_con["DeliveryChallan"].find_one(pk_query, projection)
                if pk_id_details:
                    dc_pk_id = pk_id_details['pk_id']
                    print(dc_pk_id)
                else:
                    print("No matching document found")
                inward_tablename = 'ServiceOrderGateEntry'
                inward_query = {"gsipk_table": inward_tablename, "gsisk_id": dc_id}
                inward_details = list(db_con[inward_tablename].find(inward_query))
                inward_id = '01'
                inward_ids = []
                if inward_details:
                    inward_ids = sorted([i["all_attributes"]["inwardId"].split("_IN")[-1] for i in inward_details],
                                        reverse=True)
                    inward_id = str(((2 - len(str(int(inward_ids[0]) + 1)))) * "0") + str(
                        int(inward_ids[0]) + 1) if len(
                        str(int(inward_ids[0]))) <= 2 else str(int(inward_ids[0]) + 1)
                parts = {}
                part_counter = 1
                for key in ["M_parts", "E_parts"]:
                    for part_key, part_value in result['all_attributes'][key].items():
                        unique_key = f"part{part_counter}"
                        parts[unique_key] = part_value
                        part_counter += 1
                part_key_reference = {}
                for key in parts.keys():
                    part_key_reference[parts[key]['mfr_prt_num']] = key
                if not part_key_reference:
                    return {"statusCode": 400, "body": "Something went wrong while fetching parts for the order"}
                parts_data, component_twi = {}, []
                for part, value in data['parts'].items():
                    part_number = part
                    parts_data[part_number] = value
                    pk_id = value['cmpt_id']
                    sk_timeStamp = invent_data[value['cmpt_id']]['sk_timeStamp']
                    qty = int(float(value['received_qty'])) + int(
                        float(invent_data[value['cmpt_id']]["all_attributes"]['qty']))
                    rcd = int(float(value['received_qty'])) + int(
                        float(invent_data[value['cmpt_id']]["all_attributes"].get('rcd_qty', 0)))
                    value['balance_qty'] = str(int(float(value['provided_qty'])) - int(float(value['received_qty'])))
                    db_con.Inventory.update_one(
                        {"pk_id": pk_id, "sk_timeStamp": sk_timeStamp},
                        {"$set": {"all_attributes.rcd_qty": str(rcd)}}
                    )
                print("The part data",parts_data)    
                if not parts_data:
                    return {"statusCode": 400, "body": "Cannot create gate entry without parts"}
                documents = data.get('documents', [])
                images = data.get('images', [])
                doc = {}
                for idx, docs in enumerate(documents):
                    doc_path = file_uploads.upload_file("ServiceOrderGateEntry", "PtgCms" + env_type, dc_id,
                                                        "invoice",
                                                        docs["doc_name"], docs['doc_body'])
                    doc[docs["doc_name"]] = doc_path
                image_upload = {}
                for idx, docs in enumerate(images):
                    image_path = file_uploads.upload_file("ServiceOrderGateEntry", "PtgCms" + env_type, dc_id,
                                                          "images",
                                                          docs["doc_name"], docs['doc_body'])
                    image_upload[docs["doc_name"]] = image_path
                if documents and images:
                    all_attributes = {
                        'inwardId': dc_pk_id + "_IN" + inward_id,
                        "gateEntry_id": dc_pk_id,
                        'status': inward_tablename,
                        'total_recieved_quantity': total_qty,
                        'actions': pending,
                        'dc_id': dc_id,
                        'tracking_id': data.get('tracking_id', ''),
                        'photo': image_upload,
                        'gate_entry_date': time[:10],
                        'sender_name': sender_name,
                        'sender_contact_number': sender_contact_number,
                        'parts': parts_data,
                        'documents': doc,
                        'partnerId': vendorId,
                        'rec_name': receiver_name,
                        'rec_cont': receiver_contact
                    }
                    if data.get('return_id', ''):
                        all_attributes['return_id'] = data['return_id']
                    item = {
                        "pk_id": dc_pk_id + "_IN" + inward_id,
                        "sk_timeStamp": time,
                        "all_attributes": all_attributes,
                        "gsipk_table": inward_tablename,
                        "gsisk_id": dc_id,
                        "lsi_key": '--'
                    }
                    response = db_con.ServiceOrderGateEntry.insert_one(item)
                    return {"statusCode": 200, "body": "Record for gate entry created successfully"}
                elif not documents or images:
                    all_attributes = {
                        'inwardId': dc_pk_id + "_IN" + inward_id,
                        "gateEntry_id": dc_pk_id,
                        'status': inward_tablename,
                        'total_recieved_quantity': total_qty,
                        'actions': pending,
                        'dc_id': dc_id,
                        'tracking_id': data.get('tracking_id', ''),
                        'photo': image_upload,
                        'gate_entry_date': time[:10],
                        'sender_name': sender_name,
                        'sender_contact_number': sender_contact_number,
                        'parts': parts_data,
                        'documents': doc,
                        'partnerId': vendorId,
                        'rec_name': receiver_name,
                        'rec_cont': receiver_contact
                    }
                    item = {
                        "pk_id": dc_pk_id + "_IN" + inward_id,
                        "sk_timeStamp": time,
                        "all_attributes": all_attributes,
                        "gsipk_table": inward_tablename,
                        "gsisk_id": dc_pk_id,
                        "lsi_key": '--'
                    }
                    try:
                        response = db_con.ServiceOrderGateEntry.insert_one(item)
                        print(f"Document inserted with _id: {response.inserted_id}")
                        print("")
                    except Exception as e:
                        print(f"An error occurred: {e}")
                    return {"statusCode": 200, 'body': "Record for gate entry created successfully"}
                else:
                    return {"statusCode": 400, "body": "Failed to upload the data"}
            else:
                print("It came out of result")    
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
        
    def cmsDCGetComponentsById(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            filter = {"all_attributes.dc_id": data["dc_id"]}
            result = list(db_con.DeliveryChallan.find(filter))
            if result:
                parts = {}
                part_counter = 1
                for i in result:
                    for key in ["M_parts", "E_parts"]:
                        for part_key, part_value in i['all_attributes'][key].items():
                            unique_key = f"part{part_counter}"
                            parts[unique_key] = part_value
                            part_counter += 1
                    i['all_attributes']['parts'] = parts
                result = result[0]
                pipeline_categories = {"pk_id": 1, "all_attributes": 1, "sub_categories": 1}
                categories = list(db_con.Metadata.find({}, pipeline_categories))
                category = {item['pk_id'].replace("MDID", "CTID"): item for item in categories}
                pipeline_inventory = [
                    {"$match": {"gsipk_table": "Inventory"}},
                    {"$project": {"all_attributes": 1}}
                ]
                inventory = list(db_con.Inventory.find({}, {"all_attributes": 1}))
                inventory = {item['all_attributes']['cmpt_id']: item['all_attributes'] for item in inventory}
                qatest = list(db_con.ServiceOrderGateEntry.find({"gsisk_id": data['dc_id']}, {"_id": 0, "all_attributes.parts": 1}))
                qa_ref = {}
                for entry in qatest:
                    part = entry['all_attributes']['parts']
                    for part_key, value in part.items():
                        qa_ref[value['cmpt_id']] = int(value['received_qty']) if value['cmpt_id'] not in qa_ref else \
                        qa_ref[value['cmpt_id']] + int(value['received_qty'])
                dbpart = result["all_attributes"]["parts"]
                result = [
                    {
                        "cmpt_id": result["all_attributes"]["parts"][part_key]["cmpt_id"],
                        "ctgr_id": result["all_attributes"]["parts"][part_key]["ctgr_id"],
                        "ctgr_name": result["all_attributes"]["parts"][part_key]["ctgr_name"],
                        "department": result["all_attributes"]["parts"][part_key]["department"],
                        "mfr_part_num": result["all_attributes"]["parts"][part_key]["mfr_prt_num"],
                        "part_name": category[dbpart[part_key]["ctgr_id"]]['sub_categories'][
                            inventory[dbpart[part_key]["cmpt_id"]]['sub_ctgr']],
                        "received_qty": str(int(qa_ref[result["all_attributes"]["parts"][part_key]["cmpt_id"]]))
                        if result["all_attributes"]["parts"][part_key]["cmpt_id"] in qa_ref else '0',
                        "manufacturer": inventory[result["all_attributes"]["parts"][part_key]["cmpt_id"]].get('mfr', result["all_attributes"]["parts"][part_key].get("manufacturer", "-")),
                        "description": inventory[result["all_attributes"]["parts"][part_key]["cmpt_id"]].get('description', result["all_attributes"]["parts"][part_key]["description"]),
                        "packaging": inventory[result["all_attributes"]["parts"][part_key]["cmpt_id"]].get('package', result["all_attributes"]["parts"][part_key].get("packaging","")),
                        "quantity": result["all_attributes"]["parts"][part_key]["provided_qty"],
                    }
                    if part_key.startswith("part") and
                       result["all_attributes"]["parts"][part_key]["department"] == 'Electronic'
                    else {
                        "cmpt_id": result["all_attributes"]["parts"][part_key]["cmpt_id"],
                        "ctgr_id": result["all_attributes"]["parts"][part_key]["ctgr_id"],
                        "ctgr_name": result["all_attributes"]["parts"][part_key]["ctgr_name"],
                        "department": result["all_attributes"]["parts"][part_key]["department"],
                        "mfr_part_num": result["all_attributes"]["parts"][part_key]["mfr_prt_num"],
                        "received_qty": str(int(qa_ref[result["all_attributes"]["parts"][part_key]["cmpt_id"]]))
                        if
                        result["all_attributes"]["parts"][part_key]["cmpt_id"] in qa_ref else '0',
                        "part_name": inventory[result["all_attributes"]["parts"][part_key]["cmpt_id"]].get('prdt_name', '--'),
                        "manufacturer": inventory[result["all_attributes"]["parts"][part_key]["cmpt_id"]].get('mfr', result["all_attributes"]["parts"][part_key].get("manufacturer", "-")),
                        "description": inventory[result["all_attributes"]["parts"][part_key]["cmpt_id"]].get('description', result["all_attributes"]["parts"][part_key]["description"]),
                        "packaging": inventory[result["all_attributes"]["parts"][part_key]["cmpt_id"]].get('package', result["all_attributes"]["parts"][part_key].get("packaging", "")),
                        "quantity": result["all_attributes"]["parts"][part_key]["provided_qty"],
                    }
                    for part_key in result["all_attributes"]["parts"]
                ]
                pipeline_gt_entr = [
                    {"$match": {"gsipk_table": "ServiceOrderGateEntry", "all_attributes.dc_id": data['dc_id']}},
                    {"$project": {"all_attributes.parts": 1}},
                    {"$unwind": "$all_attributes.parts"},
                    {"$replaceRoot": {"newRoot": "$all_attributes.parts"}}
                ]
                gt_entr = list(db_con.ServiceOrderGateEntry.aggregate(pipeline_gt_entr))
                total_qty = 0
                if gt_entr:
                    total_qty = sum(sum(int(part['received_qty']) for key, part in item.items()) for item in gt_entr)
            conct.close_connection(client)
            return {'statusCode': 200, 'body': result, "rcvd_qty": str(total_qty)}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(Check data)'}   
    def cmsGetServiceOrderGateEntryId(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            po_id = data['dc_id']
            result = db_con.ServiceOrderGateEntry.find({"all_attributes.dc_id": po_id},{'_id':0})
            output_result = list(result)
           
            listOfPkIds = []
            listOfPkIds = [output_result[i]["pk_id"] for i in range(len(output_result))]
            qaresult = list(db_con.ServiceOrderIQC.find({"all_attributes.inwardId": {"$in": listOfPkIds}}, {'_id': 0}))
            qaresult_ids = {item["all_attributes"]["inwardId"] for item in qaresult}
            filtered_ids = [pk_id for pk_id in listOfPkIds if pk_id not in qaresult_ids]
            if filtered_ids:  
              return {"statusCode":200,"body": filtered_ids}
            else:
              return {"statusCode":400,"body": "No data found"}
           
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'} 
    
    def cmsGetServiceOrderGateEntry(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            dc_id = data.get("dc_id")
            result = db_con.ServiceOrderGateEntry.find_one({ "all_attributes.dc_id": dc_id})
            if not result:
                return {'statusCode': 404, 'body': 'Service Order not found'}
            
            po_id = result['pk_id']
            responses = []
            data = []
            inwards = db_con.ServiceOrderGateEntry.find({'all_attributes.dc_id': dc_id})
           
            for inward in inwards:
                tick = 0
                inwardId = inward['all_attributes']['inwardId']
                print(f'The inward id{inwardId}')
                iQCData  = db_con.ServiceOrderIQC.find_one({'all_attributes.inwardId': inwardId})
                
                response = {
                    'GateEntry': {},
                    'IQC': {},
                    'inward': {}
                }
            
                if result:
                  tick += 1
                  dt = datetime.fromisoformat(inward['sk_timeStamp'])
                  ge_time = dt.strftime("%d/%m/%Y, %I:%M %p")

                  response["GateEntry"] = {
                   "gate_entry_id": inward["pk_id"],
                   "date_time": ge_time,
                   "items":len(inward["all_attributes"]["parts"]),
                   #"total_price":sum(float(part['price']) for part in i['all_attributes']['parts'].values())
                   #"total_price":"",
                   "documents":inward["all_attributes"].get("documents",{}),
                   "images":inward["all_attributes"].get("photo",{}),
                   #"invoice_num": "rdg435",
                   #"due_date": "19/09/2024"
                   
                  }

                if iQCData:
                 tick += 1 
                 dt = datetime.fromisoformat(iQCData['sk_timeStamp'])
                 qa_time = dt.strftime("%d/%m/%Y, %I:%M %p")
            
                 doc_list = []
                 for key, value in iQCData['all_attributes'].get('documents', {}).items():
                        doc_list.append({
                            'doc_name': key,
                            'doc_url': value
                        })

                 response['IQC'] = {
                     "iqc_id": iQCData["pk_id"],
                     "date_time": qa_time,
                     "items": len(iQCData['all_attributes']['parts']),
                     "total_price": 0,
                     "documents":doc_list,
                     "due_date": ""
                     #max(dates).strftime("%d/%m/%Y"),
                     #"invoice_num": "rdg435"
                 }

                 
                #get it from the ServiceOrder Inward   
                inward_data = db_con.ServiceOrderInward.find_one({'all_attributes.inwardId': inwardId})   
                if inward_data:
                   tick += 1 
                   dt = datetime.fromisoformat(inward_data['sk_timeStamp'])
                   inward_time = dt.strftime("%d/%m/%Y, %I:%M %p")     
                   response["inward"]={
                    "inw_id": inward_data["pk_id"],
                    "date_time": inward_time,
                    #"invoice_num": "",
                    "items": len(inward_data['all_attributes']['parts'])
                  }
                comments = inward.get('all_attributes', {}).get('comments', [])
                comments_data_list = []
                attachment_count = 0
                if comments:
                    for comment_key,comment in comments.items():
                        attachments_data_list = []
                        attachments_data = comment.get('attachments', [])
                        if attachments_data:
                            for attachment_key, attachment in attachments_data.items():
                                attachment_count = attachment_count + 1
                                attachments = {
                                    'file_name': attachment_key,
                                    'file_url': attachment
                                }
                                attachments_data_list.append(attachments)
                        comment_data = {
                            "comment_id": comment_key,
                            "comment_text": comment.get('comment', ''),
                            "created_time": datetime.fromisoformat(comment.get('created_time', '')).strftime("%d/%m/%Y, %I:%M %p"),
                            "attachments": attachments_data_list
                        }
                        comments_data_list.append(comment_data)
                    response["comments"] = comments_data_list
                    response["comment_count"] = len(comments_data_list)
                    response["attachment_count"] = attachment_count
                else:
                    response["comments"] = []
                    response["comment_count"] = 0
                    response["attachment_count"] = 0
                response["tick"]= tick
                
                responses.append(response)
                # "comments": [],
                #    "comment_count": 0,
                #    "attachment_count": 0,
                #  "tick": 0


            return {"statusCode": 200, "body":responses}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'failed to fetch the delivery challan details'} 
        
    def cmsSOGetComponentById(request_body):
        try:
            data = request_body #test
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            filter = {"all_attributes.dc_id": data["dc_id"]}
            result = list(db_con.NewPurchaseOrder.find(filter))
            if result:
                    result = result[0]
   
                    # pipeline_categories = {"pk_id": 1, "all_attributes": 1, "sub_categories": 1}
                    # categories = list(db_con.Metadata.find({}, pipeline_categories))
   
                    # category = {item['pk_id'].replace("MDID", "CTID"): item for item in categories}
                    inventory = list(db_con.Inventory.find({}, {"all_attributes": 1}))
                    inventory = {item['all_attributes']['cmpt_id']: item['all_attributes'] for item in inventory}
                    qatest = list(db_con.ServiceOrderGateEntry.find({"gsisk_id": data['po_id']}, {"_id": 0, "all_attributes.parts": 1}))
                    qa_ref = {}
                    for entry in qatest:
                        part = entry['all_attributes']['parts']
                        for part_key, value in part.items():
                            qa_ref[value['cmpt_id']] = int(value['received_qty']) if value['cmpt_id'] not in qa_ref else qa_ref[
                                                                                                                            value[
                                                                                                                                'cmpt_id']] + int(
                                value['received_qty'])
                    dbpart = result["all_attributes"]["purchase_list"]

                    result = [
                        {
                            "cmpt_id": result["all_attributes"]["purchase_list"][part_key]["cmpt_id"],
                            "ctgr_id": result["all_attributes"]["purchase_list"][part_key]["ctgr_id"],
                            "ctgr_name": result["all_attributes"]["purchase_list"][part_key]["ctgr_name"],
                            "department": result["all_attributes"]["purchase_list"][part_key]["department"],
                            "mfr_part_num": result["all_attributes"]["purchase_list"][part_key]["item_no"],
                            # "part_name": ""category[dbpart[part_key]["ctgr_id"]]['sub_categories'][
                            #     inventory[dbpart[part_key]["cmpt_id"]]['sub_ctgr']],
                            "received_qty": str(int(qa_ref[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]]))
                            if result["all_attributes"]["purchase_list"][part_key]["cmpt_id"] in qa_ref else '0',
                            "manufacturer": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('mfr', result[
                                "all_attributes"]["purchase_list"][part_key].get("manufacturer", "-")),
                            "description": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('description',
                                                                                                                result[
                                                                                                                    "all_attributes"][
                                                                                                                    "purchase_list"][
                                                                                                                    part_key][
                                                                                                                    "description"]),
                            "packaging": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('package',
                                                                                                            result[
                                                                                                                "all_attributes"][
                                                                                                                "purchase_list"][
                                                                                                                part_key].get(
                                                                                                                "packaging",
                                                                                                                "")),
                            "quantity": result["all_attributes"]["purchase_list"][part_key]["qty"],
                            "price": str(float(result["all_attributes"]["purchase_list"][part_key]["basic_amount"]) + float(result["all_attributes"]["purchase_list"][part_key]["gst_amount"])),
                            "price_per_piece": result["all_attributes"]["purchase_list"][part_key]["rate"]
                        }
                        if part_key.startswith("part") and
                        result["all_attributes"]["purchase_list"][part_key]["department"] == 'Electronic'
                        else {
                            "cmpt_id": result["all_attributes"]["purchase_list"][part_key]["cmpt_id"],
                            "ctgr_id": result["all_attributes"]["purchase_list"][part_key]["ctgr_id"],
                            "ctgr_name": result["all_attributes"]["purchase_list"][part_key]["ctgr_name"],
                            "department": result["all_attributes"]["purchase_list"][part_key]["department"],
                            "mfr_part_num": result["all_attributes"]["purchase_list"][part_key]["item_no"],
                            # "balance_qty":int(result["all_attributes"]["parts"][part_key]["qty"])-int(qa_ref[result["all_attributes"]["parts"][part_key]["cmpt_id"]]) if result["all_attributes"]["parts"][part_key]["cmpt_id"] in qa_ref else result["all_attributes"]["parts"][part_key]["qty"],
                            "received_qty": str(int(qa_ref[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]]))
                            if
                            result["all_attributes"]["purchase_list"][part_key]["cmpt_id"] in qa_ref else '0',
                            "part_name": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('prdt_name',
                                                                                                            '--'),
                            "manufacturer": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('mfr', result[
                                "all_attributes"]["purchase_list"][part_key].get("manufacturer", "-")),
                            "description": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('description',
                                                                                                                result[
                                                                                                                    "all_attributes"][
                                                                                                                    "purchase_list"][
                                                                                                                    part_key][
                                                                                                                    "description"]),
                            "packaging": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('package',
                                                                                                            result[
                                                                                                                "all_attributes"][
                                                                                                                "purchase_list"][
                                                                                                                part_key].get(
                                                                                                                "packaging",
                                                                                                                "")),
                            "quantity": result["all_attributes"]["purchase_list"][part_key]["qty"],
                            "price": str(float(result["all_attributes"]["purchase_list"][part_key]["basic_amount"]) + float(result["all_attributes"]["purchase_list"][part_key]["gst_amount"])),
                            "price_per_piece": result["all_attributes"]["purchase_list"][part_key]["rate"]
                        }
                        for part_key in result["all_attributes"]["purchase_list"]
                    ]
   
   
                    pipeline_gt_entr = [
                        {"$match": {"gsipk_table": "GateEntry", "all_attributes.document_id": data['po_id']}},
                        {"$project": {"all_attributes.parts": 1}},
                        {"$unwind": "$all_attributes.parts"},
                        {"$replaceRoot": {"newRoot": "$all_attributes.parts"}}
                    ]
   
                    gt_entr = list(db_con.GateEntry.aggregate(pipeline_gt_entr))
                    total_qty = 0
                    print(gt_entr)
                    if gt_entr:
                        total_qty = sum(sum(int(part['received_qty']) for key, part in item.items()) for item in gt_entr)
   
            conct.close_connection(client)
            return {'statusCode': 200, 'body': result, "rcvd_qty": str(total_qty)}



        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Something went wrong'}     
    
    def cmsServiceOrderQaTestGetDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client'] 
            doc_po_id = data['dc_id']
            gateentryId = data['gate_entry_id']
            po_tablename = 'PurchaseOrder'
            inward_tablename = 'ServiceOrderGateEntry'

            # Initialize category and inventory
            category = {}
            for item in db_con['Metadata'].find():
                pk_id = item['pk_id'].replace("MDID", "CTID")
                if item['gsipk_id'] == 'Electronic':
                    sub_categories = {key: value for key, value in item['sub_categories'].items()}
                    category[pk_id] = {"ctgr_name": item['gsisk_id'], "sub_categories": sub_categories}
                else:
                    category[pk_id] = {"ctgr_name": item['gsisk_id']}
            
            inventory = {}
            for item in db_con['Inventory'].find():
                inventory[item['pk_id']] = item['all_attributes']
                print("ggg", inventory[item['pk_id']])
            
            # Fetch the result from the database
            result = db_con[inward_tablename].find_one(
                {"all_attributes.dc_id": doc_po_id, "all_attributes.inwardId": gateentryId},
                {"_id": 0}
            )
            print("a", result)
            
            # Initialize response variable
            response = {}

            if result:
                response = {
                    'inwardId': result['all_attributes']['inwardId'],
                    'tracking_id': result['all_attributes']['tracking_id'],
                    'sender_name': result['all_attributes']['sender_name'],
                    'partnerId': result['all_attributes']['partnerId'],
                    'sender_contact_number': result['all_attributes']['sender_contact_number'],
                    'parts': []
                }

                for key, value in result['all_attributes']['parts'].items():
                    if value['received_qty'] != 0:  # Exclude parts where received_qty is 0
                        part_data = {
                            "s_no": key.replace("part", ""),
                            "ctgr_id": value['ctgr_id'],
                            "cmpt_id": value['cmpt_id'],
                            "part_no": value['mfr_prt_num'],
                            "part_name": category[value['ctgr_id']]['sub_categories'][inventory[value['cmpt_id']]['sub_ctgr']]
                                if value['department'] == 'Electronic' else inventory[value['cmpt_id']].get('prdt_name', ''),
                            "department": value['department'],
                            "received_qty": value['received_qty'],
                            # "price": value['price'],
                            "price": value.get('price',''),
                            "description": inventory[value['cmpt_id']].get('description', ''),
                            "packaging": inventory[value['cmpt_id']].get('package', ''),
                            "manufacturer": inventory[value['cmpt_id']].get('mfr', '-'),
                            "pass_qty": "0",
                            "fail_qty": "0"
                        }
                        response['parts'].append(part_data)

            # Close the connection
            conct.close_connection(client)

            # Return the response
            return {'statusCode': 200, 'body': response}
        
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Something went wrong'}

    
    # def cmsServiceOrderQaTestGetDetails(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         client = db_conct['client'] 
    #         doc_po_id = data['dc_id']
    #         # inwardId = data['inwardId']
    #         gateentryId=data['gate_entry_id']
    #         po_tablename = 'PurchaseOrder'
    #         inward_tablename = 'ServiceOrderGateEntry'

    #         # Initialize category and inventory
    #         category = {}
    #         for item in db_con['Metadata'].find():
    #             pk_id = item['pk_id'].replace("MDID", "CTID")
    #             if item['gsipk_id'] == 'Electronic':
    #                 sub_categories = {key: value for key, value in item['sub_categories'].items()}
    #                 category[pk_id] = {"ctgr_name": item['gsisk_id'], "sub_categories": sub_categories}
    #             else:
    #                 category[pk_id] = {"ctgr_name": item['gsisk_id']}
            
    #         inventory = {}
    #         for item in db_con['Inventory'].find():
    #             inventory[item['pk_id']] = item['all_attributes']
    #             print("ggg",inventory[item['pk_id']])
            
    #         # Fetch the result from the databaseq
    #         result = db_con[inward_tablename].find_one({"all_attributes.dc_id": doc_po_id,"all_attributes.inwardId":gateentryId},{"_id": 0})
    #         print("a",result)
            
    #         # Initialize response variable to ensure it exists, "pk_id": inwardId
    #         response = {}

    #         if result:
    #             po_id = result.get("gsisk_id")
    #             response = {
    #                 # 'order_no': po_id,
    #                 'inwardId': result['all_attributes']['inwardId'],
    #                 # 'QA_date': str(datetime.today()),
    #                 # 'invoice_photo': {i: file_get(result['all_attributes']['photo'][i]) for i in result['all_attributes']['photo']},
    #                 # 'invoice_document': file_get(result['all_attributes']['invoice']),
    #                 'tracking_id': result['all_attributes']['tracking_id'],
    #                 'sender_name': result['all_attributes']['sender_name'],
    #                 'partnerId': result['all_attributes']['partnerId'],
    #                 'sender_contact_number': result['all_attributes']['sender_contact_number'],
    #                 'parts': []
    #             }

    #             for key, value in result['all_attributes']['parts'].items():
    #                 if value['received_qty'] != '0':
    #                     part_data = {
    #                         "s_no": key.replace("part", ""),
    #                         "ctgr_id": value['ctgr_id'],
    #                         "cmpt_id": value['cmpt_id'],
    #                         "part_no": value['mfr_prt_num'],
    #                         # "part_name": category[value['ctgr_id']]['sub_categories'][inventory[value['cmpt_id']]['sub_ctgr']]
    #                         # if value['department'] == 'Electronic' else inventory[value['cmpt_id']].get('prdt_name', ''),
    #                         "part_name": category[value['ctgr_id']]['sub_categories'][inventory[value['cmpt_id']]['sub_ctgr']]
    #                         if value['department'] == 'Electronic' else inventory[value['cmpt_id']].get('prdt_name', ''),
    #                         # "part_name": category[value['ctgr_id']][inventory[value['cmpt_id']]]
    #                         # if value['department'] == 'Mechanic' else inventory[value['cmpt_id']].get('prdt_name', ''),
    #                         # "unit_price": value['unit_price'],
    #                         "department": value['department'],
    #                         # "delivery_date": value["delivery_date"],
    #                         "received_qty": value['received_qty'],
    #                         "price": value['price'],
    #                         "description": inventory[value['cmpt_id']].get('description', ''),
    #                         "packaging": inventory[value['cmpt_id']].get('package', ''),
    #                         "manufacturer": inventory[value['cmpt_id']].get('mfr', '-'),
    #                         "pass_qty": "0",
    #                         "fail_qty": "0"
    #                     }
    #                     response['parts'].append(part_data)

    #         # Close the connection
    #         conct.close_connection(client)

    #         # Return the response
    #         return {'statusCode': 200, 'body': response}
        
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400, 'body': 'Something went wrong'}
    
    

    def cmsServiceOrderSaveIQCTest(request_body):
        try:

            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sk_timeStamp = (datetime.now()).isoformat()

            gsipk_table = "ServiceOrderIQC"
            qa_id = data['inwardId'].split('IN')[-1]
            gate_id = data["inwardId"]

            # Retrieve gate entry
            gateEntry = list(db_con.ServiceOrderGateEntry.find({
                "$and": [{"pk_id": gate_id}]},
                {"_id": 0, "all_attributes": 1, "pk_id": 1, "sk_timeStamp": 1}
            ))

            if gateEntry:
                # Generate pk_id
                max_qa_entry = db_con[gsipk_table].find_one(
                    {"pk_id": {"$regex": f"{gate_id.split('_')[0]}_QAID"}},  # Modify regex to match new format
                    sort=[("pk_id", -1)]
                )

                if max_qa_entry:
                    last_qa_id = int(max_qa_entry['pk_id'].split('_QAID')[-1])
                    qa_id = last_qa_id + 1
                else:
                    qa_id = 1

                # Updated pk_id format
                pk_id = f"{gate_id.split('_')[0]}_QAID{qa_id}"  # Modify pk_id to follow new format

                # Process parts
                parts = gateEntry[0]['all_attributes']['parts']
                quant_ref = {part['cmpt_id']: int(part['received_qty']) for part in data['parts']}

                if any(quant_ref[part['cmpt_id']] != int(part['pass_qty']) + int(part['fail_qty']) for part in data['parts']):
                    return {'statusCode': 404, 'body': 'Pass qty and fail qty do not match the quantity for some components'}

                processed_parts = [
                    {
                        "cmpt_id": part_data["cmpt_id"],
                        "ctgr_id": part_data['ctgr_id'],
                        "mfr_prt_num": part_data["part_no"],
                        "prdt_name": part_data["part_name"],
                        "description": part_data["description"],
                        "department": part_data["department"],
                        "packaging": part_data["packaging"],
                        "qty": str(part_data["received_qty"]),
                        "delivery_date": part_data.get('delivery_date', '-'),
                        "price": part_data["price"],
                        "price_per_piece": part_data.get("unit_price", '-'),
                        "manufacturer": part_data.get('manufacturer', '-'),
                        "pass_qty": part_data["pass_qty"],
                        "gst": part_data.get('gst', '-'),
                        "fail_qty": part_data["fail_qty"],
                        "lot_id": part_data.get("lot_id", '-')
                    }
                    for part_data in data["parts"]
                ]

                # Process documents
                docs = {}
                if "documents" in data:
                    for doc in data["documents"]:
                        file_path = file_uploads.upload_file(
                            "purchaseOrder", "PtgCms" + env_type, doc["doc_name"].split('.')[-1],
                            gate_id, doc["doc_name"], doc["doc_body"]
                        )
                        docs[doc["doc_name"]] = file_path

                all_attributes = {
                    "parts": {f'part{i + 1}': processed_parts[i] for i in range(len(processed_parts))},
                    "partnerId": gateEntry[0]["all_attributes"]["partnerId"],
                    'actions': 'inward',
                    "sender_name": data["sender_name"],
                    "dc_id": data["dc_id"],
                    "sender_contact_number": data["sender_contact_number"],
                    "description": data["description"],
                    "tracking_id":data["tracking_id"],
                    "sk_timeStamp": sk_timeStamp,
                    "inwardId": data["inwardId"],
                    "gate_entry_date": gateEntry[0]["all_attributes"]["gate_entry_date"],
                    "documents": docs  # Add documents to all_attributes
                }

                # Insert into the database
                qa_item = {
                    "pk_id": pk_id,
                    "sk_timeStamp": sk_timeStamp,
                    "all_attributes": all_attributes,
                    "gsipk_table": gsipk_table,
                    "gsisk_id": gate_id,
                    "lsi_key": "status_QA-Test"
                }
                db_con[gsipk_table].insert_one(qa_item)

                # Close the connection
                conct.close_connection(client)

                return {'statusCode': 200, 'body': 'IQC added successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Something went wrong'}
        
    def getIQCIds(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            
            result = db_con.ServiceOrderIQC.find({"all_attributes.dc_id": data['dc_id']},{'_id':0})
            output_result = list(result)
            listOfPkIds = []
            listOfPkIds = [output_result[i]["all_attributes"]["inwardId"] for i in range(len(output_result))]
            inwardResult = list(db_con.ServiceOrderInward.find({"all_attributes.inwardId": {"$in": listOfPkIds}}, {'_id': 0}))
            inward_result_ids = {item["all_attributes"]["inwardId"] for item in inwardResult}
            filtered_ids = [pk_id for pk_id in listOfPkIds if pk_id not in inward_result_ids]
            final_iqc_ids = []
            for iqcid in filtered_ids:
                result = db_con.ServiceOrderIQC.find_one({'all_attributes.inwardId': iqcid}, {'_id': 0})
                final_iqc_ids.append(result.get('pk_id', ''))
            if final_iqc_ids:
                return {'statusCode': 200, 'body': final_iqc_ids}
            else:
                return {'statusCode': 400, 'body': "no record found"}
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'failed to fetch the IQC ids'} 
        
    def getIQCDataByID(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            iqc_id = data['iqc_id']
            
            parts_data_list = []
            parts_data = {}
            result = db_con.ServiceOrderIQC.find_one({'pk_id': data['iqc_id']},{'_id':0, 'all_attributes': 1})
            if result:
                parts = result.get('all_attributes', {}).get('parts', {})
                for key, part in parts.items():
                    parts_data = {
                        'cmpt_id': part['cmpt_id'],
                        'ctgr_id': part['ctgr_id'],
                        'mfr_prt_num': part['mfr_prt_num'],
                        "prdt_name": part['prdt_name'],
                        "description": part['description'],
                        "department": part['department'],
                        "packaging": part['packaging'],
                        "qty": part['qty'],
                        "delivery_date": part['delivery_date'],
                        "price": part['price'],
                        "price_per_piece": part['price_per_piece'],
                        "delivery_date": part['delivery_date'],
                        "manufacturer": part['manufacturer'],
                        "pass_qty": part['pass_qty'],
                        "gst": part['gst'],
                        "fail_qty": part['fail_qty'],
                        "lot_id": part['lot_id']
                    }
                    parts_data_list.append(parts_data)
                    all_attributes = {
                        'partnerId': result.get('all_attributes', {}).get('partnerId', ''),
                        'sender_name': result.get('all_attributes', {}).get('sender_name', ''),
                        'dc_id': result.get('all_attributes', {}).get('dc_id', ''),
                        'tracking_id': result.get('all_attributes', {}).get('tracking_id', ''),
                        'description': result.get('all_attributes', {}).get('description', ''),
                        'inwardId': result.get('all_attributes', {}).get('inwardId', ''),
                        'parts': parts_data_list, 
                    }
                return {'statusCode': 200, 'body': all_attributes}
            else:
                return {'statusCode': 400, 'body': "no data found"}
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'failed to fetch the IQC data'} 
        
    def CmsServiceOrderSaveInward(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sk_timeStamp = (datetime.now()).isoformat()
            gsipk_table = "ServiceOrderInward"
            inward_query = {"gsipk_table": gsipk_table}
            inward_data = db_con[gsipk_table].find_one(inward_query)
            inwardedId = data['inwardId'].split("IN")[-1]
            gate_id = data["inwardId"]
            
            # Retrieve gate entry
            gateEntry = list(db_con.ServiceOrderGateEntry.find({
                "$and": [{"pk_id": gate_id}]},
                {"_id": 0, "all_attributes": 1, "pk_id": 1, "sk_timeStamp": 1}
            ))

            if gateEntry:
                # Generate pk_id
                max_inward_entry = db_con[gsipk_table].find_one(
                    {"pk_id": {"$regex": f"{gate_id.split('_')[0]}_INWARDId"}},  # Modify regex to match new format
                    sort=[("pk_id", -1)]
                )

                if max_inward_entry:
                    last_inward_id = int(max_inward_entry['pk_id'].split('_INWARDId')[-1])
                    qa_id = last_inward_id + 1
                else:
                    qa_id = 1

             # Updated pk_id format
            pk_id = f"{gate_id.split('_')[0]}_INWARDId{qa_id}"  # Modify pk_id to follow new format
            print(pk_id)
            print(data)
            parts = []
            for part_data in data.get("parts", []):
                print("kalyan",part_data)
                part = {
                    "mfr_prt_num": part_data.get("mfr_prt_num", "-"),
                    "cmpt_id": part_data.get("cmpt_id", ""),
                    "ctgr_id": part_data.get("ctgr_id", ""),
                    "prdt_name": part_data.get("prdt_name", ""),
                    "description": part_data.get("description", ""),
                    "packaging": part_data.get("packaging", ""),
                    "inventory_position": part_data.get("inventory_position", ""),
                    "manufacturer": part_data.get("manufacturer", ""),
                    "qty": part_data.get("qty", ""),
                    "pass_qty": part_data.get("pass_qty", ""),
                    "fail_qty": part_data.get("fail_qty", ""),
                    "batchId": part_data.get("batchId", ""),
                    "lot_no": part_data.get("lot_no", "testlot"),
                    "lot_id": part_data.get("lot_id", "-")
                }
                parts.append(part)
            all_attributes = {
                "parts": {f'part{i + 1}': parts[i] for i in range(len(parts))},
                "partnerId": data["partnerId"],
                "sender_name": data["sender_name"],
                "dc_id": data["dc_id"],
                "description": data["description"],
                "tracking_id": data["tracking_id"],
                "inward_date": data["inward_date"],
                "inwardId": data["inwardId"],
                "actions": 'inwarded',
            }
            
            item = {
                "pk_id": pk_id,
                "sk_timeStamp": sk_timeStamp,
                "all_attributes": all_attributes,
                "gsipk_table": gsipk_table,
                "gsisk_id": '',
                "lsi_key": "status_inward"
            }
            
            # # INSERTING INWARDED ITEM
            db_con[gsipk_table].insert_one(item)
                
            response = {'statusCode': 200, 'body': "Inward added successfully"}
            return response
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}
        
    def getSOGateEntryPopUp(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            get_gate_entry_data = db_con.ServiceOrderGateEntry.find_one({"pk_id": data["gate_entry_id"]}, {"_id": 0, 'all_attributes': 1})
            if get_gate_entry_data is not None:
                gate_entry_all_attributes_data = get_gate_entry_data['all_attributes']
                return {'statusCode': 200, 'body': gate_entry_all_attributes_data}
            else:
                return {'statusCode': 400, 'body': 'No record found'}
                        
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Getting Gate Entry Popup Failed'}
        
    def getSOInwardPopUp(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            inward_data = db_con.ServiceOrderInward.find_one({"pk_id": data["inward_id"]}, {"_id": 0, 'all_attributes': 1})
            if inward_data is not None:
                inward_all_attributes_data = inward_data['all_attributes']
                return {'statusCode': 200, 'body': inward_all_attributes_data}
            else:
                return {'statusCode': 400, 'body': 'No record found'}
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Getting Inward Popup Failed'}
    
    def getSOIQCPopUp(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            iqc_data = db_con.ServiceOrderIQC.find_one({"pk_id": data["iqc_id"]}, {"_id": 0, 'all_attributes': 1})
            if iqc_data is not None:
                iqc_all_attributes_data = iqc_data["all_attributes"]
                return {'statusCode': 200, 'body': iqc_all_attributes_data}
            else:
                return {'statusCode': 400, 'body': 'No record found'}
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Getting IQC Popup Failed'}
        
    def saveCommentsForServiceOrderGateEntry(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            dc_id = data['dc_id']
            inward_id = data['inward_id']
            attachment_dict = {}
            if "attachment" in data.keys() and data["attachment"]:
                for attachment in data['attachment']:
                    document_body = attachment['doc_body']
                    document_name = attachment['doc_name']
                    extra_type = ""
                    attachment_url = file_uploads.upload_file("GateEntry", "PtgCms" + env_type, extra_type, "GateEntry" + dc_id, document_name, document_body)
                    attachment_dict[document_name] = attachment_url  # Adds or updates the dictionary entry for each attachment
            else:
                attachment_dict = {}
            comment_data = {
                "created_time": datetime.now().isoformat(),
                "comment": data["comment"],
                "attachments": attachment_dict  # This dictionary now contains all attachments
            }
            gate_entry_result = db_con.ServiceOrderGateEntry.find_one({"pk_id": inward_id}, {"_id": 0, "all_attributes": 1})
            comments_dict = gate_entry_result.get('all_attributes', {}).get('comments', {})
            if not isinstance(comments_dict, dict):
                if isinstance(comments_dict, list):
                    comments_dict = {f"comment{i + 1}": v for i, v in enumerate(comments_dict)}
                else:
                    comments_dict = {}
            new_comment_key = f"comment{len(comments_dict) + 1}"
            # Add the new comment to the dictionary
            comments_dict[new_comment_key] = comment_data
            db_con.ServiceOrderGateEntry.update_one(
                    {"pk_id": inward_id},
                    {"$set": {"all_attributes.comments": comments_dict}}
            )
            result = list(db_con.GateEntry.find({"pk_id": inward_id}, {"_id": 0}))
            return{'statusCode': 200, 'body': 'Comment added successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Comment adding failed'}
               
   
    def CmsSoGetComponentsById(request_body):
            try:
                data = request_body #test
                env_type = data['env_type']
                db_conct = conct.get_conn(env_type)
                db_con = db_conct['db']
                client = db_conct['client']
                filter = {"all_attributes.dc_id": data["dc_id"]}
                result = list(db_con.DeliveryChallan.find(filter))
                # print(result)
                if result:
                    result = result[0]
   
                    # pipeline_categories = {"pk_id": 1, "all_attributes": 1, "sub_categories": 1}
                    # categories = list(db_con.Metadata.find({}, pipeline_categories))
   
                    # category = {item['pk_id'].replace("MDID", "CTID"): item for item in categories}
   
                    pipeline_inventory = [
                        {"$match": {"gsipk_table": "Inventory"}},
                        {"$project": {"all_attributes": 1}}
                    ]
                    inventory = list(db_con.Inventory.find({}, {"all_attributes": 1}))
                    inventory = {item['all_attributes']['cmpt_id']: item['all_attributes'] for item in inventory}
   
                    qatest = list(db_con.ServiceOrderGateEntry.find({"gsisk_id": data['dc_id']}, {"_id": 0, "all_attributes.parts": 1}))
   
                    qa_ref = {}
                    for entry in qatest:
                        part = entry['all_attributes']['parts']
                        for part_key, value in part.items():
                            qa_ref[value['cmpt_id']] = int(value['received_qty']) if value['cmpt_id'] not in qa_ref else qa_ref[
                                                                                                                            value[
                                                                                                                                'cmpt_id']] + int(
                                value['received_qty'])
                    #dbpart = result["all_attributes"]["purchase_list"]
                    result = [
                        {
                            "cmpt_id": result["all_attributes"]["purchase_list"][part_key]["cmpt_id"],
                            "ctgr_id": result["all_attributes"]["purchase_list"][part_key]["ctgr_id"],
                            "ctgr_name": result["all_attributes"]["purchase_list"][part_key]["ctgr_name"],
                            "department": result["all_attributes"]["purchase_list"][part_key]["department"],
                            "mfr_part_num": result["all_attributes"]["purchase_list"][part_key]["item_no"],
                            # "part_name": category[dbpart[part_key]["ctgr_id"]]['sub_categories'][
                            #     inventory[dbpart[part_key]["cmpt_id"]]['sub_ctgr']],
                            "received_qty": str(int(qa_ref[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]]))
                            if result["all_attributes"]["purchase_list"][part_key]["cmpt_id"] in qa_ref else '0',
                            "manufacturer": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('mfr', result[
                                "all_attributes"]["purchase_list"][part_key].get("manufacturer", "-")),
                            "description": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('description',
                                                                                                                result[
                                                                                                                    "all_attributes"][
                                                                                                                    "purchase_list"][
                                                                                                                    part_key][
                                                                                                                    "description"]),
                            "packaging": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('package',
                                                                                                            result[
                                                                                                                "all_attributes"][
                                                                                                                "purchase_list"][
                                                                                                                part_key].get(
                                                                                                                "packaging",
                                                                                                                "")),
                            "quantity": result["all_attributes"]["purchase_list"][part_key]["qty"],
                            "price": str(float(result["all_attributes"]["purchase_list"][part_key]["basic_amount"]) + float(result["all_attributes"]["purchase_list"][part_key]["gst_amount"])),
                            "price_per_piece": result["all_attributes"]["purchase_list"][part_key]["rate"]
                        }
                        if part_key.startswith("part") and
                        result["all_attributes"]["purchase_list"][part_key]["department"] == 'Electronic'
                        else {
                            "cmpt_id": result["all_attributes"]["purchase_list"][part_key]["cmpt_id"],
                            "ctgr_id": result["all_attributes"]["purchase_list"][part_key]["ctgr_id"],
                            "ctgr_name": result["all_attributes"]["purchase_list"][part_key]["ctgr_name"],
                            "department": result["all_attributes"]["purchase_list"][part_key]["department"],
                            "mfr_part_num": result["all_attributes"]["purchase_list"][part_key]["item_no"],
                            # "balance_qty":int(result["all_attributes"]["parts"][part_key]["qty"])-int(qa_ref[result["all_attributes"]["parts"][part_key]["cmpt_id"]]) if result["all_attributes"]["parts"][part_key]["cmpt_id"] in qa_ref else result["all_attributes"]["parts"][part_key]["qty"],
                            "received_qty": str(int(qa_ref[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]]))
                            if
                            result["all_attributes"]["purchase_list"][part_key]["cmpt_id"] in qa_ref else '0',
                            "part_name": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('prdt_name',
                                                                                                            '--'),
                            "manufacturer": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('mfr', result[
                                "all_attributes"]["purchase_list"][part_key].get("manufacturer", "-")),
                            "description": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('description',
                                                                                                                result[
                                                                                                                    "all_attributes"][
                                                                                                                    "purchase_list"][
                                                                                                                    part_key][
                                                                                                                    "description"]),
                            "packaging": inventory[result["all_attributes"]["purchase_list"][part_key]["cmpt_id"]].get('package',
                                                                                                            result[
                                                                                                                "all_attributes"][
                                                                                                                "purchase_list"][
                                                                                                                part_key].get(
                                                                                                                "packaging",
                                                                                                                "")),
                            "quantity": result["all_attributes"]["purchase_list"][part_key]["qty"],
                            "price": str(float(result["all_attributes"]["purchase_list"][part_key]["basic_amount"]) + float(result["all_attributes"]["purchase_list"][part_key]["gst_amount"])),
                            "price_per_piece": result["all_attributes"]["purchase_list"][part_key]["rate"]
                        }
                        for part_key in result["all_attributes"].get("purchase_list","")
                    ]
   
   
                    pipeline_gt_entr = [
                        {"$match": {"gsipk_table": "GateEntry", "all_attributes.dc_id": data['dc_id']}},
                        {"$project": {"all_attributes.parts": 1}},
                        {"$unwind": "$all_attributes.parts"},
                        {"$replaceRoot": {"newRoot": "$all_attributes.parts"}}
                    ]
   
                    gt_entr = list(db_con.ServiceOrderGateEntry.aggregate(pipeline_gt_entr))
                    total_qty = 0
                    print(gt_entr)
                    if gt_entr:
                        total_qty = sum(sum(int(part['received_qty']) for key, part in item.items()) for item in gt_entr)
                    print(f"The total{total_qty}")
                conct.close_connection(client)
                return {'statusCode': 200, 'body': result}
            except Exception as err:
                exc_type, exc_obj, tb = sys.exc_info()
                f_name = tb.tb_frame.f_code.co_filename
                line_no = tb.tb_lineno
                print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
                return {'statusCode': 400, 'body': 'Bad Request(Check data)'}
            
    def saveSoEolBinaryBuildDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            sk_timeStamp = (datetime.now()).isoformat()
            
            get_builds = list(db_con.EolBinary.find({}))
            all_attributes = {
                "build_name": data.get('build_name', ''),
                "build_number": data.get('build_number', ''),
                "build_link": data.get('build_link', ''),
                "so_id": data.get('so_id', '')
            }
            item = {
                "pk_id": f'EOLBinary{len(get_builds) + 1}',
                "sk_timeStamp": sk_timeStamp,
                "all_attributes": all_attributes,
                "gsipk_table": "EolBinary",
                "gsisk_id": '',
                "lsi_key": ""
            }
            db_con.EolBinary.insert_one(item)
            return{'statusCode': 200, 'body': 'EOL Binary details added successfully'}
        
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'EOL Binary details adding failed'}
        
    def getSoEolBinaryBuildDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            sk_timeStamp = (datetime.now()).isoformat()
            
            get_builds = list(db_con.EolBinary.find({"all_attributes.so_id": data.get('so_id', '')}, {'_id': 0}))
            sorted_data = sorted(get_builds, key=lambda x: x["pk_id"], reverse=True)
            eol_builds_array = []
            for build_data in sorted_data:
                all_attributes = {
                    "build_name": build_data.get('all_attributes', {}).get('build_name', ''),
                    "build_number": build_data.get('all_attributes', {}).get('build_number', ''),
                    "build_link": build_data.get('all_attributes', {}).get('build_link', ''),
                    "so_id": build_data.get('all_attributes', {}).get('so_id', ''),
                    "status": "new" if eol_builds_array == [] else "old",
                    "created_at": sk_timeStamp
                }
                eol_builds_array.append(all_attributes)
            if eol_builds_array:
                return{'statusCode': 200, 'body': eol_builds_array}
            else:
                return{'statusCode': 400, 'body': 'No records found'}
        
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Getting EOL Binary details failed'}
        
    def cmsPartnersCreateProductionLine(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sk_timeStamp = (datetime.now()).isoformat()
            so_id = data['so_id']
            record_type = data['type']
            existing_document = db_con.ProductionLine.find_one(
                {"all_attributes.so_id": so_id, "all_attributes.type": record_type})
            if existing_document:
                existing_boards = existing_document['all_attributes'].get("boards", {})
                max_board_num = max(
                    [int(board_key.replace("board", "")) for board_key in existing_boards.keys()],
                    default=0
                )
                new_boards = data['boards']
                updated_boards = {}
                for i, (key, board_data) in enumerate(new_boards.items(), start=max_board_num + 1):
                    board_data_with_status = {**board_data, "status": False}
                    updated_boards[f"board{i}"] = board_data_with_status
                db_con.ProductionLine.update_one(
                    {"all_attributes.so_id": so_id, "all_attributes.type": record_type},
                    {"$set": {
                        "all_attributes.boards": {**existing_boards, **updated_boards},
                        "sk_timeStamp": sk_timeStamp
                    }}
                )
                return {'statusCode': 200, 'body': 'Production line updated with new boards'}
            else:
                latest_record = db_con.ProductionLine.find_one(
                    sort=[("pk_id", -1)]
                )
                if latest_record and re.match(r"PL\d{3}", latest_record["pk_id"]):
                    max_pk_id_num = int(latest_record["pk_id"].split("PL")[1])
                    new_pk_id = f"PL{max_pk_id_num + 1:03}"
                else:
                    new_pk_id = "PL001"
                boards = {f"board{i + 1}": {**board_data, "status": False} for i, (key, board_data) in
                          enumerate(data['boards'].items())}
                all_attributes = {
                    "so_id": so_id,
                    "type": record_type,
                    "boards": boards
                }
                production_line_data = {
                    "pk_id": new_pk_id,
                    "sk_timeStamp": sk_timeStamp,
                    "all_attributes": all_attributes,
                    "gsipk_table": "ProductionLine",
                    "gsipk_id": record_type,
                    "lsi_key": "--"
                }
                db_con.ProductionLine.insert_one(production_line_data)
                return {'statusCode': 200, 'body': 'Production line created successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request (check data)'}
        
    def cmsPartnerGetProductionLine(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            res = list(db_con.ProductionLine.find({"$and": [
                {'all_attributes.so_id': data['so_id']},
                {'all_attributes.type': data['type']}
            ]}))
            result = []
            for record in res:
                boards = record.get('all_attributes', {}).get('boards', {})
                result.extend(boards.values())
            return {'statusCode': 200, 'body': result}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request (check data)'}
 
        
 
 
 
   
 
 
 
               
