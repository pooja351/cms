import json
from datetime import datetime, timedelta
import base64
from db_connection import db_connection_manage
import sys
import os
import re

conct = db_connection_manage()

class quotation:
    
    def getPTGAddress(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            from_address = {}
            from_address["from_address"] = {
                        "company_name": "People Tech IT Consultancy Pvt Ltd",
                        "gst_number": "36AAGCP2263H2ZE",
                        "pan_number": " AAGCP2263H",
                        "address": "Plot No.14 & 15, RMZ Futura Building, Block B, Hitech City, Hyderabad,Telangana, India- 500081"
                    }
            return {'statusCode': 200, 'body': from_address}
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400,'body': 'Getting Address Failed'}
        
    def saveQuotationForm(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            sk_timeStamp = (datetime.now()).isoformat()
            current_date = datetime.fromisoformat(sk_timeStamp)
            document_month = datetime.strptime(current_date.strftime('%Y-%m-%d') , '%Y-%m-%d').strftime('%m')
            document_year = datetime.strptime(current_date.strftime('%Y-%m-%d'), '%Y-%m-%d').strftime('%y')
            next_year = str(int(document_year) + 1).zfill(2)
            document_month_year = f"{document_month}/{document_year}-{next_year}"
          
            quotation_products_list = data.get('quotation_products_list', [])
            quotation_product = {}
            for index, product in enumerate(quotation_products_list, start=1):
                quotation_product['product'+str(index)] = {
                    "s_no": product['s_no'],
                    "item_description": product['item_description'],
                    "part_no": product['part_no'],
                    "qty": product['qty'],
                    "gst_per": product['gst_per'],
                    "unit_cost": product['unit_cost'],
                    "total_cost": product['total_cost'],
                    "gst_amount": product['gst_amount']
                }
            pk_ids = list(db_con.Quotations.find({'pk_id': {'$regex': '^QUOTE'}}, {'pk_id': 1}))
            if len(pk_ids) == 0: 
                pk_id = "QUOTEPTG1"
                max_pk = 1
            else:
                pk_filter = [int(x['pk_id'][8:]) for x in pk_ids]
                max_pk = max(pk_filter) + 1
                pk_id = "PIPTG" + str(max_pk)
            quotation_num = f"EPL/QUOTE/{max_pk}/{document_month_year}"
            all_attributes = {
                'ship_to': data.get('to_address', {}),
                'from_address': data.get('from_address', {}),
                'req_line': data.get('req_line', ''),
                'primary_document_details': data.get('primary_document_details', {}), 
                'secondary_doc_details': data.get('secondary_document_details', {}),
                'bank_details': data.get('bank_details', {}),
                'quotation_products_list': quotation_product,
                'total_amount': data.get('total_amount', {}),
                'quote_id': quotation_num
            }
            quotation_data = {
                'pk_id': f'QUOTEPTG{max_pk}',
                'created_on': sk_timeStamp,
                'all_attributes': all_attributes,
                'gsipk_table': 'Quotations',
                'gsipk_id': '',
                'lsi_key': 'pending'
            }
            result = db_con.Quotations.insert_one(quotation_data)
            return {'statusCode': 200, 'body': 'Quotation added successfully'}
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400,'body': 'Quotation Creation Failed'}
        
    def getQuotationById(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            quotation_data = db_con.Quotations.find_one({'all_attributes.quote_id': data.get('quote_id', '')}, {'_id': 0, 'all_attributes': 1})
            if quotation_data:
                 return {'statusCode': 200, 'body': quotation_data['all_attributes']}
            else:
                 return {'statusCode': 400, 'body': "No Record Found"}
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400,'body': 'Getting Quotation details Failed'}
        
    def updateQuotationForm(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            sk_timeStamp = (datetime.now()).isoformat()
            updatestatus = data["updatestatus"]
            
            new_status = "Pending" if updatestatus == "Rejected" else updatestatus
            quotation_products_list = data.get('quotation_products_list', [])
            quotation_product = {}
            for index, product in enumerate(quotation_products_list, start=1):
                quotation_product['product'+str(index)] = {
                    "s_no": product['s_no'],
                    "item_description": product['item_description'],
                    "part_no": product['part_no'],
                    "qty": product['qty'],
                    "gst_per": product['gst_per'],
                    "unit_cost": product['unit_cost'],
                    "total_cost": product['total_cost'],
                    "gst_amount": product['gst_amount']
                }
            all_attributes = {
                'ship_to': data.get('to_address', {}),
                'from_address': data.get('from_address', {}),
                'req_line': data.get('req_line', ''),
                'primary_document_details': data.get('primary_document_details', {}), 
                'secondary_doc_details': data.get('secondary_document_details', {}),
                'bank_details': data.get('bank_details', {}),
                'quotation_products_list': quotation_product,
                'total_amount': data.get('total_amount', {}),
                'quote_id': data.get('quote_id', ''),
                'updated_on': sk_timeStamp
            }
            print(new_status)
            result = db_con.Quotations.update_one(
                {'all_attributes.quote_id': data['quote_id']},
                {"$set": {
                          'all_attributes': all_attributes,
                          'lsi_key': new_status
                          }
                }
            )
            if result.matched_count == 0:
                return {'statusCode': 400, 'body': 'Quotation Updation failed'}
            else:
                return {'statusCode': 200, 'body': 'Quotation Updated successfully'}            
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400,'body': 'Quotation updation failed'}
        
    def saveDraftQuotationForm(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            sk_timeStamp = (datetime.now()).isoformat()
            current_date = datetime.fromisoformat(sk_timeStamp)
            document_month = datetime.strptime(current_date.strftime('%Y-%m-%d') , '%Y-%m-%d').strftime('%m')
            document_year = datetime.strptime(current_date.strftime('%Y-%m-%d'), '%Y-%m-%d').strftime('%y')
            next_year = str(int(document_year) + 1).zfill(2)
            document_month_year = f"{document_month}/{document_year}-{next_year}"
            
            quotation_products_list = data.get('quotation_products_list', [])
            quotation_product = {}
            for index, product in enumerate(quotation_products_list, start=1):
                quotation_product['product'+str(index)] = {
                    "s_no": product['s_no'],
                    "item_description": product['item_description'],
                    "part_no": product['part_no'],
                    "qty": product['qty'],
                    "gst_per": product['gst_per'],
                    "unit_cost": product['unit_cost'],
                    "total_cost": product['total_cost'],
                    "gst_amount": product['gst_amount']
                }
            pk_ids = list(db_con.DraftQuotations.find({'pk_id': {'$regex': '^DRAFTQUOTE'}}, {'pk_id': 1}))
            if len(pk_ids) == 0: 
                max_pk = 1
            else:
                pk_filter = [int(x['pk_id'][13:]) for x in pk_ids]
                max_pk = max(pk_filter) + 1
            quotation_num = f"EPL/DRAFTQUOTE/{max_pk}/{document_month_year}"
            all_attributes = {
                'ship_to': data.get('to_address', {}),
                'from_address': data.get('from_address', {}),
                'req_line': data.get('req_line', ''),
                'primary_document_details': data.get('primary_document_details', {}), 
                'secondary_doc_details': data.get('secondary_document_details', {}),
                'bank_details': data.get('bank_details', {}),
                'quotation_products_list': quotation_product,
                'total_amount': data.get('total_amount', {}),
                'quote_id': quotation_num
            }
            quotation_data = {
                'pk_id': f'DRAFTQUOTEPTG{max_pk}',
                'created_on': sk_timeStamp,
                'all_attributes': all_attributes,
                'gsipk_table': 'DraftQuotations',
                'gsipk_id': '',
                'lsi_key': 'pending'
            }
            result = db_con.DraftQuotations.insert_one(quotation_data)
            return {'statusCode': 200, 'body': 'Draft Quotation added successfully'}
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400,'body': 'Draft Quotation creation failed'}
        
    def getDraftQuotationById(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            draft_quotation_data = db_con.DraftQuotations.find_one({'all_attributes.quote_id': data.get('draft_quote_id', '')}, {'_id': 0, 'all_attributes': 1})
            if draft_quotation_data:
                 return {'statusCode': 200, 'body': draft_quotation_data['all_attributes']}
            else:
                 return {'statusCode': 400, 'body': "No Record Found"}
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400,'body': 'Getting Draft quotation details failed'}
        
    def updateDraftQuotationForm(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            sk_timeStamp = (datetime.now()).isoformat()
            
            pk_ids = list(db_con.Quotations.find({'pk_id': {'$regex': '^QUOTE'}}, {'pk_id': 1}))
            if len(pk_ids) == 0: 
                pk_id = "QUOTEPTG1"
                max_pk = 1
            else:
                pk_filter = [int(x['pk_id'][8:]) for x in pk_ids]
                max_pk = max(pk_filter) + 1
                pk_id = "QUOTEPTG" + str(max_pk)
            draft_quote_no = data["draft_quote_id"]
            draft_data = db_con.DraftQuotations.find_one({"all_attributes.quote_id": draft_quote_no})
            if not draft_data:
                return {'statusCode': 404, 'body': 'Draft quotation not found'}
            
            quotation_products_list = data.get('quotation_products_list', [])
            quotation_product = {}
            for index, product in enumerate(quotation_products_list, start=1):
                quotation_product['product'+str(index)] = {
                    "s_no": product['s_no'],
                    "item_description": product['item_description'],
                    "part_no": product['part_no'],
                    "qty": product['qty'],
                    "gst_per": product['gst_per'],
                    "unit_cost": product['unit_cost'],
                    "total_cost": product['total_cost'],
                    "gst_amount": product['gst_amount']
                }
            all_attributes = {
                'ship_to': data.get('to_address', {}),
                'from_address': data.get('from_address', {}),
                'req_line': data.get('req_line', ''),
                'primary_document_details': data.get('primary_document_details', {}), 
                'secondary_doc_details': data.get('secondary_document_details', {}),
                'bank_details': data.get('bank_details', {}),
                'quotation_products_list': quotation_product,
                'total_amount': data.get('total_amount', {}),
                'quote_id': data.get('quote_id', ''),
            }
            draft_date = all_attributes["primary_document_details"].get("document_date", "")
         
            if draft_date:
                try:
                    quote_date_obj = datetime.strptime(draft_date, "%Y-%m-%d")
                    quote_month = quote_date_obj.month
                    quote_year = quote_date_obj.strftime("%y")
                    quote_next = (quote_date_obj.year + 1) % 100
                    quote_id = f"EPL/QUOTE/{max_pk}/{quote_month}/{quote_year}-{quote_next:02d}"
                    all_attributes["quote_id"] = quote_id
                except ValueError:
                    return {'statusCode': 400, 'body': 'Invalid Quote date format'}
            else:
                    return {'statusCode': 400, 'body': 'Quote Date is required'}
            quotation_data = {
                'pk_id': f'QUOTEPTG{max_pk}',
                'created_on': sk_timeStamp,
                'all_attributes': all_attributes,
                'gsipk_table': 'Quotations',
                'gsipk_id': '',
                'lsi_key': 'pending'
            }
            result = db_con.Quotations.insert_one(quotation_data)
            db_con.DraftQuotations.delete_one({'all_attributes.quote_id': draft_quote_no})
            return {'statusCode': 200, 'body': 'Draft Quotation updated successfully'}            
            
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400,'body': 'Draft Quotation updation failed'}
        