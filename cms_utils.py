import binascii
import os, mimetypes
import base64
from base64 import b64decode, b64encode
import boto3
import re

class file_uploads():

    def upload_file(feature_name, environ, type, id, filename, document_content):
        try:
            if document_content == '' or str(document_content).startswith("http"):
                return document_content
            
            # Decode the document content if it's base64 encoded
            document_content = base64.b64decode(document_content)
            
            aws_access_key_id = "AKIA4F52A4XRXLIKFYOH"
            aws_secret_access_key = "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"
            region_name = "us-west-1"
            bucket_name = 'cms-image-data'
            
            # Initialize the S3 client
            s3 = boto3.client(
                's3', 
                aws_access_key_id=aws_access_key_id, 
                aws_secret_access_key=aws_secret_access_key, 
                region_name=region_name
            )
            
            dir_name = environ + "/" + feature_name
            if type:
                key = f"{dir_name}/{type}/{id}/{filename}"
            else:
                key = f"{dir_name}/{id}/{filename}"
            
            # Determine the content type based on the file extension
            if filename.lower().endswith('.jpg') or filename.lower().endswith('.jpeg'):
                ContentType = "image/jpeg"
            elif filename.lower().endswith('.pdf'):
                ContentType = "application/pdf"
            elif filename.endswith('.csv'):
                ContentType = "text/csv"
            elif filename.lower().endswith('.xls') or filename.lower().endswith('.xlsx') :
                ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            else:
                ContentType = "application/octet-stream"
            
            # Upload the file to S3
            s3.put_object(Bucket=bucket_name, Key=key, Body=document_content, ContentType=ContentType)
            
            # Generate the file URL
            s3_url = f"https://{bucket_name}.s3.us-west-1.amazonaws.com/"
            fileurl = s3_url + key
            
            return fileurl
        except Exception as e:
            print("Error:", e)
            return ''


    # def upload_file(feature_name, environ, type, id, filename, document_content):
    #     try:
    #         if document_content=='' or str(document_content).startswith("http"):
    #             return document_content
    #         document_content = base64.b64decode(document_content)
    #         aws_access_key_id = "AKIA4F52A4XRXLIKFYOH"
    #         aws_secret_access_key = "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"
    #         region_name = "us-west-1"
    #         bucket_name = 'cms-image-data'
    #         s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    #         # s3 = boto3.client('s3',region_name=region_name)
    #         dir_name = environ + "/"+feature_name
    #         if type:
    #             key = dir_name + "/" + type +"/"+id+"/" + filename
    #         else:
    #             key = dir_name + "/" +id+"/" + filename
    #         # key = dir_name + "/" + type +"/"+id+"/" + filename
    #         ContentType = "Image/jpeg" if "jpg" in filename else "application/pdf"
    #         s3.put_object(Bucket=bucket_name, Key=key, Body=document_content, ContentType=ContentType)
    #         s3_url = f"https://{bucket_name}.s3.us-west-1.amazonaws.com/"
    #         fileurl = s3_url + key
    #         # print("File uploaded successfully to S3:",fileurl)
    #         return fileurl
    #     except Exception as e:
    #         print("Error:", e)
    #         return ''
    
    def delete_file_from_s3(key):
        try:
            bucket_name = 'cms-image-data'
            key = key.replace(f"https://{bucket_name}.s3.us-west-1.amazonaws.com/",'')
            s3_client = boto3.client('s3')
            s3_client.delete_object(Bucket='cms-image-data', Key=key)
            return True
        except Exception as e:
            return False
        
    def get_file_single_image(path):
        return path#base64_data

    def get_file(paths):
        # print(paths)
        base64_data_list = []
        for path_dict in paths:
            file_path = path_dict['content']
            document_name = path_dict['document_name']
            base64_data_list.append({"content": file_path, "document_name": document_name})
        return base64_data_list

    def upload_excel_and_csv_files(feature_name, environ, type, id, filename, document_content):
        try:
            if document_content == '' or str(document_content).startswith("http"):
                return document_content
            document_content = base64.b64decode(document_content)
            aws_access_key_id = "AKIA4F52A4XRXLIKFYOH"
            aws_secret_access_key = "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"
            region_name = "us-west-1"
            bucket_name = 'cms-image-data'
            s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                              region_name=region_name)
            dir_name = f"{environ}/{feature_name}"
            # print(filename)
            key = f"{dir_name}/{type}/{id}/{filename}" if type else f"{dir_name}/{id}/{filename}"
            if filename.endswith('.xlsx'):
                content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif filename.endswith('.csv'):
                content_type = "text/csv"
            elif filename.endswith('.pdf'):
                content_type = "application/pdf"
            elif filename.endswith('.jpg') or filename.endswith('.jpeg'):
                content_type = "image/jpeg"
            else:
                content_type = "application/octet-stream"
            s3.put_object(Bucket=bucket_name, Key=key, Body=document_content, ContentType=content_type)
            s3_url = f"https://{bucket_name}.s3.{region_name}.amazonaws.com/"
            fileurl = s3_url + key
            return fileurl
        except Exception as e:
            print("Error:", e)
            return ''
        
    def upload_bulk_files(feature_name, environ, type, id, filename, document_content):
        try:
            if document_content == '' or str(document_content).startswith("http"):
                return document_content
            document_content = base64.b64decode(document_content)
            aws_access_key_id = "AKIA4F52A4XRXLIKFYOH"
            aws_secret_access_key = "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"
            region_name = "us-west-1"
            bucket_name = 'cms-image-data'
            s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                              region_name=region_name)
            dir_name = f"{environ}/{feature_name}"
            key = f"{dir_name}/{type}/{id}/{filename}" if type else f"{dir_name}/{id}/{filename}"
            content_type = mimetypes.guess_type(filename)[0]
            if not content_type:
                content_type = "application/octet-stream"
            s3.put_object(Bucket=bucket_name, Key=key, Body=document_content, ContentType=content_type)
            s3_url = f"https://{bucket_name}.s3.{region_name}.amazonaws.com/"
            fileurl = s3_url + key
            return fileurl
        except Exception as e:
            print("Error:", e)
            return ''

class dynamic_fields():
    def inventory_reference(db_con, select_parameters, query):
        inventory = list(db_con.Inventory.find(query, select_parameters))
        inventory_ref = {item['all_attributes']['cmpt_id']: item for item in inventory}
        return inventory_ref

    def metadata_reference(db_con, select_parameters, query):
        metadata = list(db_con.Metadata.find(query, select_parameters))
        metadata_ref = {(item['pk_id'].replace("MDID", "CTID")): item for item in metadata}
        return metadata_ref

    def bom_reference(db_con, select_parameters, query):
        boms = list(db_con.Boms.find(query, select_parameters))
        boms_ref = {item['bom_id']: item for item in boms}
        return boms_ref

    def vendor_reference(db_con, select_parameters, query):
        vendors = list(db_con.Vendors.find(query, select_parameters))
        vendors_ref = {item['vendor_id']: item for item in vendors}
        return vendors_ref

def batch_number_allocation(batch_number_info, qty, cmpt_id, ekit_info):
    batch_string = ""
    lot_no = ''
    invoice_no = ''
    po_id = ''
    if batch_number_info:
        if ekit_info:
            existing_qty = int(ekit_info[cmpt_id])  # Ensure existing_qty is an integer
            for key, value in batch_number_info[cmpt_id].items():
                if existing_qty > 0:
                    value_qty = int(value['qty'])  # Ensure value['qty'] is an integer
                    if value_qty > existing_qty:
                        batch_number_info[cmpt_id][key]['qty'] = f"{value_qty - existing_qty}"
                    else:
                        batch_number_info[cmpt_id][key]['qty'] = "0"
                    existing_qty = existing_qty - value_qty if value_qty <= existing_qty else 0
        for inx, item in batch_number_info[cmpt_id].items():
            if item['qty'] != '0':
                item_qty = int(item['qty'])  # Ensure item['qty'] is an integer
                if int(qty) <= item_qty:
                    batch_number_info[cmpt_id][inx]['qty'] = f"{item_qty - int(qty)}"
                    return {
                        "batch_number_info": batch_number_info,
                        "batch_string": (batch_string + f"{item['batch_number']},")[0:-1],
                        "lot_no": item.get('lot_no', ""),
                        "invoice_no": item.get('invoice_no', ""),
                        "po_id": item['pk_id'].split("_")[0]
                    }
                else:
                    batch_string += f"{item['batch_number']},"
                    lot_no = lot_no + "," + item.get('lot_no', "")
                    invoice_no = invoice_no + "," + item.get("invoice_no", "")
                    if item['pk_id'].split("_")[0] not in po_id:
                        po_id = po_id + "," + item['pk_id'].split("_")[0]
                    qty = int(qty) - item_qty
                    batch_number_info[cmpt_id][inx]['qty'] = "0"
            else:
                continue
        if int(qty) > 0:
            return False
    else:
        if int(qty) > 0:
            return False
    return {"batch_number_info": batch_number_info, "batch_string": "-", "lot_no": "", "invoice_no": "", "po_id": ""}


# def batch_number_allocation(batch_number_info,qty,cmpt_id,ekit_info):
#     batch_string = ""
#     lot_no = ''
#     invoice_no = ''
#     po_id = ''
#     if batch_number_info:
#         if ekit_info:
#             # var = batch_number_allocation(batch_number_info,existing_qty,cmpt_id,{})
#             # batch_number_info = var['batch_number_info']
#             existing_qty = ekit_info[cmpt_id]
#             for key,value in batch_number_info[cmpt_id].items():
#                 if existing_qty>0:
#                     qty = int(value['qty'])
#                     if qty>existing_qty:
#                         batch_number_info[cmpt_id][key]['qty'] = f"{qty-existing_qty}"
#                     else:
#                         batch_number_info[cmpt_id][key]['qty'] = "0"
#                     existing_qty=existing_qty-qty if qty<=existing_qty else 0 
#         for inx,item in batch_number_info[cmpt_id].items():
#             if item['qty']!='0':
#                 if int(qty)<=int(item['qty']):
#                     batch_number_info[cmpt_id][inx]['qty'] =  f"{int(item['qty'])-int(qty)}"
#                     return {
#                         "batch_number_info":batch_number_info,
#                         "batch_string":(batch_string+f"{item['batch_number']},")[0:-1],
#                         "lot_no":item.get('lot_no',""),
#                         "invoice_no":item.get('invoice_no',""),
#                         "po_id":item['pk_id'].split("_")[0]
#                         }
#                 else:
#                     batch_string+=f"{item['batch_number']},"
#                     lot_no = lot_no + "," + item.get('lot_no',"")
#                     invoice_no = invoice_no + "," + item.get("invoice_no","")
#                     if item['pk_id'].split("_")[0] not in po_id:
#                         po_id = po_id + "," +item['pk_id'].split("_")[0]
#                     if int(qty)<=int(item['qty']):
#                         batch_number_info[cmpt_id][inx]['qty'] =  f"{int(item['qty'])-int(qty)}"
#                         return {
#                             "batch_number_info":batch_number_info,
#                             "batch_string":batch_string[0:-1],
#                             "lot_no":item.get('lot_no',""),
#                             "invoice_no":invoice_no,
#                             "po_id":po_id
#                             }
#                     else:
#                         qty=int(qty)-int(item['qty'])
#             else:
#                 continue
#         if int(qty)>0:
#             return False
#     else:
#         if int(qty):
#             return False
#         return {"batch_number_info":batch_number_info,"batch_string":"-","lot_no":"","invoice_no":"","po_id":""}
 
 
def find_stock_inwards(bom_id,db_con,parts_type):
    price_bom = db_con.BOM.find_one({'pk_id':bom_id},{"_id":0,f"all_attributes.price_bom.{parts_type}":1})
    price_bom = price_bom['all_attributes']['price_bom'][parts_type]
    vendors = [value['vendor_id'] for key,value in price_bom.items()]
    part_batch_info = {}
    poids = ''
    for vendor in vendors:
        pos = (db_con.PurchaseOrder.find_one({"all_attributes.bom_id":bom_id,'all_attributes.vendor_id':vendor},{"_id":0,"pk_id":1}))
        if pos:
            inwards = list(db_con.inward.find({"all_attributes.po_id":pos['pk_id']},{"_id":0,"all_attributes.parts":1,"pk_id":1,"all_attributes.invoice_num":1}))
            if inwards:
                for inward in inwards:
                    parts = inward['all_attributes']['parts']
                    for key,value in parts.items():
                        poids+=pos['pk_id']
                        if value['cmpt_id'] not in part_batch_info:
                            part_batch_info[value['cmpt_id']] = {value['batchId']:{'batch_number':value['batchId'],"qty":value['pass_qty'],"key":key,"pk_id":inward['pk_id'],"invoice_no":inward['all_attributes']['invoice_num'],"lot_no":value['lot_no']}}
                        else:
                            part_batch_info[value['cmpt_id']][value['batchId']] = {'batch_number':value['batchId'],"qty":value['pass_qty'],"key":key,"pk_id":inward['pk_id'],"invoice_no":inward['all_attributes']['invoice_num'],"lot_no":value['lot_no']}
            else:
                None
        else:
            return None
    return {"part_batch_info":part_batch_info}

def find_stock_inward_new(bom_id,db_con,parts_type):
    price_bom = db_con.BOM.find_one({'pk_id':bom_id},{"_id":0,f"all_attributes.price_bom.{parts_type}":1})
    if not price_bom:
        return {'statusCode': 404, 'body': f'Document with BOM id "{bom_id}" not found'}
    if 'all_attributes' not in price_bom:
        return {'statusCode': 404, 'body': f"'all_attributes' key not found in BOM with id \"{bom_id}\""}
    if 'price_bom' not in price_bom['all_attributes']:
        return {'statusCode': 404, 'body': f"'price_bom' key not found in 'all_attributes' for BOM with id \"{bom_id}\""}
    if parts_type not in price_bom['all_attributes']['price_bom']:
        return {'statusCode': 404, 'body': f'price_bom for parts_type "{parts_type}" not found in BOM with id "{bom_id}"'}
    price_bom = price_bom['all_attributes']['price_bom'][parts_type]
    vendors = [value['vendor_id'] for key,value in price_bom.items()]
    part_batch_info = {}#test
    poids = ''
    for vendor in vendors:
        # pos = (db_con.PurchaseOrder.find_one({"all_attributes.bom_id":bom_id,'all_attributes.vendor_id':vendor},{"_id":0,"pk_id":1}))
        purchase_orders = db_con.PurchaseOrder.find({"all_attributes.bom_id": bom_id, 'all_attributes.vendor_id': vendor}, {"_id": 0, "pk_id": 1})
        for pos in purchase_orders:
            inwards = list(db_con.inward.find({"all_attributes.po_id":pos['pk_id']},{"_id":0,"all_attributes.parts":1,"pk_id":1,"all_attributes.invoice_num":1}))
            if inwards:
                for inward in inwards:
                    parts = inward['all_attributes']['parts']
                    for key,value in parts.items():
                        poids+=pos['pk_id']
                        if value['cmpt_id'] not in part_batch_info:
                            part_batch_info[value['cmpt_id']] = {value['batchId']:{'batch_number':value['batchId'],"qty":value['pass_qty'],"key":key,"pk_id":inward['pk_id'],"invoice_no":inward['all_attributes']['invoice_num'],"lot_no":value['lot_no']}}
                        else:
                            part_batch_info[value['cmpt_id']][value['batchId']] = {'batch_number':value['batchId'],"qty":value['pass_qty'],"key":key,"pk_id":inward['pk_id'],"invoice_no":inward['all_attributes']['invoice_num'],"lot_no":value['lot_no']}
    return {"part_batch_info":part_batch_info}

def get_kit_and_boards_info(data, partner_type):
    cmpt_info = {}
    pattern = r'E-KIT\d+' if partner_type == 'EMS' else r'M_KIT\d+'
    for key in data.keys():
        if re.match(pattern, key):
            for sub_key, value in data[key].items():
                if 'cmpt_id' in value and 'provided_qty' in value:
                    cmpt_id = value['cmpt_id']
                    qty = value['provided_qty']
                    if cmpt_id in cmpt_info:
                        cmpt_info[cmpt_id] = str(int(cmpt_info[cmpt_id]) + int(qty))
                    else:
                        cmpt_info[cmpt_id] = qty
    return cmpt_info

# def get_kit_and_boards_info(data,partner_type):
#     cmpt_info = {}
#     for key in data.keys():
#         pattern = r'E-KIT\d+' if partner_type=='EMS' else r'M_KIT\d+'
#         if re.match(pattern,key):
#             for key,value in data[key].items():
#                 cmpt_id = value['cmpt_id']
#                 qty = value['provided_qty']
#                 if cmpt_id in cmpt_info:
#                     cmpt_info[cmpt_id] = f"{int(cmpt_info[cmpt_id])+int(qty)}"
#                 else:
#                     cmpt_info[cmpt_id] = qty
#     return cmpt_info