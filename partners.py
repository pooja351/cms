import json
from datetime import datetime,timedelta
import base64
from db_connection import db_connection_manage
import sys
import os
from bson import ObjectId


conct = db_connection_manage()

class Partners():
    def CmsPartnerCreate(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            sk_timeStamp = (datetime.now() + timedelta(hours=5, minutes=30)).isoformat()
            partners = list(db_con.Partners.find({}))
            # #print(partners)
            partners_id = "0001"
            if partners:
                # #print(partners)
                partners_ids = [i["partnerId"] for i in partners]
                partners_ids.sort(reverse=True)
                numeric_part = int(partners_ids[0].split("R")[1]) + 1
                partners_id = str(numeric_part).zfill(4)
                # #print(partners_id)
            documents=data['documents']
            doc = {}
            for inx, docs in enumerate(documents):
                img = docs['content']
                image_type = docs["document_name"]
                if img:
                    image_64_decode = base64.b64decode(img)
                    directory = f"../../../cms-images/vendor/{data['type']}/{partners_id}/"
                    image_path = os.path.join(directory, image_type)
                    # Create directory if it doesn't exist
                    os.makedirs(directory, exist_ok=True)
                    with open(image_path, 'wb') as image_result:
                        image_result.write(image_64_decode)
                    doc[image_type] = image_path
            data["documents"]=doc
            # #print(data)
            partner_data = {
                "pk_id": "PTGPAR" + partners_id,
                "sk_timeStamp": sk_timeStamp,
                "all_attributes": data,
                "gsipk_id": data['type'],
                "gsipk_table":"Partner",
                "lsi_key":"Active"
            }
            db_con.Partners.insert_one(partner_data)
            conct.close_connection(client)
            return {'statusCode': 200, 'body': f'New Partner {data["name"]} created successfully'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Vendor deletion failed'}
        

    def cmsGetDCIDForPartnerInvoice(request_body):
      try:  
        data = request_body
        eny_type = data['env_type']
        db_conct = conct.get_conn(eny_type)
        db_con = db_conct['db']
        client = db_conct['client']
        query = {
                "gsipk_table":"DeliveryChallan",
                "gsisk_id":"open"
               
            }
        projection = {
                "all_attributes.dc_id": 1,
                "_id": 0
            }
        res = list(db_con.DeliveryChallan.find(query, projection))
        datas = []
        for i in res:
                datas.append(i['all_attributes']['dc_id'])
        if datas:
                response = {
                    'statusCode':200,
                    "body":datas
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
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Vendor deletion failed'}
     
    def cmsCreatePartnerInvoice(request_body):
       
       try:
        data = request_body
        env_type = data['env_type']
        db_conct = conct.get_conn(env_type)
        db_con = db_conct['db']
        time = (datetime.now()).isoformat()
        all_attributes = {
            "buyer_details":data.get("buyer_details"),
            "delivery_location":data.get("delivery_location"),
            "supplier_details":data.get("supplier_details"),
            "placeof_supply":data.get("placeof_supply"),
            "primary_doc_details": data.get("primary_document_details"),
            "productlistDetails":data.get("productlistDetails")
        }
        item = {
                        "sk_timeStamp": time,
                        "all_attributes": all_attributes,
                        "gsipk_table": "partnerInvoice",
                        "gsisk_id": "",
                        "lsi_key": '--'
        }
        try:
                        response = db_con.PartnerInvoice.insert_one(item)
                        print(f"Document inserted with _id: {response.inserted_id}")
                        print("")
        except Exception as e:
                        print(f"An error occurred: {e}")
 
        return {"statusCode": 200, 'body': "Record for patner Invoice created successfully"}
 
       except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400,'body': 'Vendor deletion failed'}
 

    def cmsPartnersGetOutwardList(request_body):
        # try:
        data = request_body
        env_type = data['env_type']
        db_conct = conct.get_conn(env_type)
        db_con = db_conct['db']
        client = db_conct['client']
        #print(data)

        data_base_table_name = 'PtgCms'+data["env_type"]
        partner_id=data["partner_id"]
        #print("hello")
        if data["dep_type"]=="EMS":
            # statement = f"""select all_attributes."order_date",all_attributes."outward_id",all_attributes."bom_id",all_attributes."qty",all_attributes."mtrl_prcnt",all_attributes."partner_id"
            # from {data_base_table_name} where  gsipk_table = 'EMS' AND all_attributes.partner_id='{partner_id}' """


            # partner_data = extract_items_from_array_of_nested_dict(execute_statement_with_pagination(statement))

            # partner_data1=list(db_con.EMS.find({},{"all_attributes.partner_id":partner_id}))

            partner_data=list(db_con.EMS.find({"all_attributes.partner_id":partner_id},{'_id':0,"all_attributes.order_date":1,"all_attributes.outward_id":1,"all_attributes.bom_id":1,"all_attributes.qty":1,"all_attributes.mtrl_prcnt":1,"all_attributes.partner_id":1}))
            bom_ids = [partner_data[i]['all_attributes']['bom_id'] for i in range(len(partner_data))]
            query_result = list(db_con.BOM.find({'pk_id': {'$in': bom_ids}}, {'_id': 0, 'pk_id': 1, 'all_attributes.bom_name': 1}))
            bom_dict = {item['pk_id']: item['all_attributes']['bom_name'] for item in query_result}
            flattened_partner_data = []
            for item in partner_data:
                flattened_item = {
                    "order_date": item["all_attributes"]["order_date"],
                    "outward_id": item["all_attributes"]["outward_id"],
                    "bom_id": item["all_attributes"]["bom_id"],
                    "bom_name":bom_dict[item["all_attributes"]["bom_id"]],
                    "qty": item["all_attributes"]["qty"],
                    "mtrl_prcnt": item["all_attributes"]["mtrl_prcnt"],
                    "partner_id": item["all_attributes"]["partner_id"]
                }
                flattened_partner_data.append(flattened_item)
            if flattened_partner_data:
                flattened_partner_data = sorted(flattened_partner_data, key=lambda x: int(x['outward_id'].replace("BTOUT", "")), reverse=False)
                #print(partner_data)
                return  {"statusCode": 200,"body": flattened_partner_data }
            else:
                #print("no data")
                return {"statusCode":404,"body":"NO DATA FOUND"}
        if data["dep_type"] == "BOXBUILDING" :
            #print("gchfgh")
            # statement = f"""select all_attributes."order_date",all_attributes."outward_id",all_attributes."bom_id",all_attributes."qty",all_attributes."partner_id"
            # from {data_base_table_name} where  gsipk_table = 'BoxBuilding' AND all_attributes.partner_id='{partner_id}' """
            # partner_data = extract_items_from_array_of_nested_dict(execute_statement_with_pagination(statement))
            flattened_partner_data = []
            partner_data=list(db_con.BoxBuilding.find({"all_attributes.partner_id":partner_id},{"all_attributes.date":1,"all_attributes.outward_id":1,"all_attributes.bom_id":1,"all_attributes.qty":1,"all_attributes.partner_id":1}))
            # return partner_data[0]
            if partner_data:
                updated_partner_data = [{**d['all_attributes'], "mtrl_prcnt": "--", 
                                         "order_date": d['all_attributes']['date']
                                         } for d in partner_data]
                updated_partner_data = sorted(updated_partner_data, key=lambda x: int(x['outward_id'].replace("BTOUT", "")), reverse=False)
                for i in updated_partner_data:
                    bom_id = i.get('bom_id')
                    bom = db_con.BOM.find_one({'all_attributes.bom_id': bom_id})
                    if bom:
                        i['bom_name'] = bom.get('all_attributes', {}).get('bom_name', '') 
                # return (updated_partner_data)
                return  {"statusCode": 200,"body": updated_partner_data }
            # for item in partner_data:
            #     flattened_item = {
            #         "order_date": item["all_attributes"]["date"],
            #         "outward_id": item["all_attributes"]["outward_id"],
            #         "bom_id": item["all_attributes"]["bom_id"],
            #         "qty": item["all_attributes"]["qty"],
            #         # "mtrl_prcnt": item["all_attributes"]["mtrl_prcnt"],
            #         "partner_id": item["all_attributes"]["partner_id"]
            #     }
            #     flattened_partner_data.append(flattened_item)
            # if flattened_partner_data:
            #     flattened_partner_data = [{**d, "mtrl_prcnt": "--", "order_date": "--"} for d in flattened_partner_data]
            #     updated_partner_data = sorted(flattened_partner_data, key=lambda x: int(x['outward_id'].replace("BTOUT", "")), reverse=False)

                # return (updated_partner_data)
                return  {"statusCode": 200,"body": updated_partner_data }
        else:
            #print("no data")
            return {"statusCode":404,"body":"NO DATA FOUND"}
        # except Exception as err:
        #     exc_type, exc_obj, tb = sys.exc_info()
        #     f_name = tb.tb_frame.f_code.co_filename
        #     line_no = tb.tb_lineno
        #     #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
        #     return {'statusCode': 400,'error': 'There is an AWS Lambda Data Capturing Error'}
    # def cmsPartnersGetOutwardList(request_body):
    #     # try:
    #     data = request_body
    #     env_type = data['env_type']
    #     db_conct = conct.get_conn(env_type)
    #     db_con = db_conct['db']
    #     client = db_conct['client']
    #     #print(data)

    #     data_base_table_name = 'PtgCms'+data["env_type"]
    #     partner_id=data["partner_id"]
    #     #print("hello")
    #     if data["dep_type"]=="EMS":
    #         # statement = f"""select all_attributes."order_date",all_attributes."outward_id",all_attributes."bom_id",all_attributes."qty",all_attributes."mtrl_prcnt",all_attributes."partner_id"
    #         # from {data_base_table_name} where  gsipk_table = 'EMS' AND all_attributes.partner_id='{partner_id}' """


    #         # partner_data = extract_items_from_array_of_nested_dict(execute_statement_with_pagination(statement))

    #         # partner_data1=list(db_con.EMS.find({},{"all_attributes.partner_id":partner_id}))

    #         partner_data=list(db_con.EMS.find({"all_attributes.partner_id":partner_id},{'_id':0,"all_attributes.order_date":1,"all_attributes.outward_id":1,"all_attributes.bom_id":1,"all_attributes.qty":1,"all_attributes.mtrl_prcnt":1,"all_attributes.partner_id":1}))
    #         flattened_partner_data = []
    #         for item in partner_data:
    #             flattened_item = {
    #                 "order_date": item["all_attributes"]["order_date"],
    #                 "outward_id": item["all_attributes"]["outward_id"],
    #                 "bom_id": item["all_attributes"]["bom_id"],
    #                 "qty": item["all_attributes"]["qty"],
    #                 "mtrl_prcnt": item["all_attributes"]["mtrl_prcnt"],
    #                 "partner_id": item["all_attributes"]["partner_id"]
    #             }
    #             flattened_partner_data.append(flattened_item)
    #         if flattened_partner_data:
    #             flattened_partner_data = sorted(flattened_partner_data, key=lambda x: int(x['outward_id'].replace("BTOUT", "")), reverse=False)
    #             #print(partner_data)
    #             return  {"statusCode": 200,"body": flattened_partner_data }
    #         else:
    #             #print("no data")
    #             return {"statusCode":404,"body":"NO DATA FOUND"}
    #     if data["dep_type"] == "BOXBUILDING" :
    #         #print("gchfgh")
    #         # statement = f"""select all_attributes."order_date",all_attributes."outward_id",all_attributes."bom_id",all_attributes."qty",all_attributes."partner_id"
    #         # from {data_base_table_name} where  gsipk_table = 'BoxBuilding' AND all_attributes.partner_id='{partner_id}' """
    #         # partner_data = extract_items_from_array_of_nested_dict(execute_statement_with_pagination(statement))
    #         flattened_partner_data = []
    #         partner_data=list(db_con.BoxBuilding.find({"all_attributes.partner_id":partner_id},{"all_attributes.date":1,"all_attributes.outward_id":1,"all_attributes.bom_id":1,"all_attributes.qty":1,"all_attributes.partner_id":1}))
    #         # return partner_data[0]
    #         if partner_data:
    #             updated_partner_data = [{**d['all_attributes'], "mtrl_prcnt": "--", 
    #                                      "order_date": d['all_attributes']['date']
    #                                      } for d in partner_data]
    #             updated_partner_data = sorted(updated_partner_data, key=lambda x: int(x['outward_id'].replace("BTOUT", "")), reverse=False)
              
    #             # return (updated_partner_data)
    #             return  {"statusCode": 200,"body": updated_partner_data }
    #         # for item in partner_data:
    #         #     flattened_item = {
    #         #         "order_date": item["all_attributes"]["date"],
    #         #         "outward_id": item["all_attributes"]["outward_id"],
    #         #         "bom_id": item["all_attributes"]["bom_id"],
    #         #         "qty": item["all_attributes"]["qty"],
    #         #         # "mtrl_prcnt": item["all_attributes"]["mtrl_prcnt"],
    #         #         "partner_id": item["all_attributes"]["partner_id"]
    #         #     }
    #         #     flattened_partner_data.append(flattened_item)
    #         # if flattened_partner_data:
    #         #     flattened_partner_data = [{**d, "mtrl_prcnt": "--", "order_date": "--"} for d in flattened_partner_data]
    #         #     updated_partner_data = sorted(flattened_partner_data, key=lambda x: int(x['outward_id'].replace("BTOUT", "")), reverse=False)

    #             # return (updated_partner_data)
    #             return  {"statusCode": 200,"body": updated_partner_data }
    #     else:
    #         #print("no data")
    #         return {"statusCode":404,"body":"NO DATA FOUND"}
    #     # except Exception as err:
    #     #     exc_type, exc_obj, tb = sys.exc_info()
    #     #     f_name = tb.tb_frame.f_code.co_filename
    #     #     line_no = tb.tb_lineno
    #     #     #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #     #     return {'statusCode': 400,'error': 'There is an AWS Lambda Data Capturing Error'}

    # def cmsPartnerGetStock(request_body):
    #     try:
    #         data = request_body
    #         env_type = data.get('env_type')
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         client = db_conct['client']
    #         partner_id = data['partner_id']
    #         status_type = data['status']
    #         query = list(db_con.Partners.find({'pk_id': partner_id}, {'_id': 0}))
    #         total_price = list(db_con.PurchaseOrder.find())
    #         stock_details_list = []
    #         if query:
    #             if status_type == "EMS":
    #                 stock_details_list = [item['all_attributes']['available_stock']['E-Kit'] for item in query]
    #                 mfr_part_numbers = []
    #                 for item in stock_details_list:
    #                     for part in item.values():
    #                         mfr_part_numbers.append(part.get('mfr_part_number', None))
    #                 result = []
    #                 for i in total_price:
    #                     if "all_attributes" in i and "parts" in i["all_attributes"]:
    #                         parts = i["all_attributes"]["parts"]
    #                         res = {}
    #                         for part_key in parts:
    #                             if part_key.startswith('part'):
    #                                 if "mfr_prt_num" in i["all_attributes"]["parts"][part_key]:
    #                                     if i["all_attributes"]["parts"][part_key]['mfr_prt_num'] in mfr_part_numbers:
    #                                         res['unit'] = i["all_attributes"]["parts"][part_key]["unit_price"]
    #                                         res['timestamp'] = i["sk_timeStamp"]
    #                                         res['mfr_prt_num'] = i["all_attributes"]["parts"][part_key]['mfr_prt_num']
    #                                         result.append(res)
    #                 max_units = {}
    #                 for item in result:
    #                     mfr_prt_num = item['mfr_prt_num']
    #                     unit = item['unit']
    #                     # if mfr_prt_num not in max_units or int(unit) > int(max_units[mfr_prt_num]):
    #                     #     max_units[mfr_prt_num] = unit
    #                     if isinstance(unit, str):
    #                         try:
    #                             unit = float(unit)
    #                         except ValueError:
    #                             unit = 0  # or handle the error as needed
    #                     if mfr_prt_num not in max_units or float(unit) > float(max_units[mfr_prt_num]):
    #                         max_units[mfr_prt_num] = unit
                            
    #                 for parts in stock_details_list:
    #                     for _, part in parts.items():
    #                         mfr_part_number = part['mfr_part_number']
    #                         part['unit_price'] = max_units.get(mfr_part_number, '-')
    #                 response = [part for parts in stock_details_list for key, part in parts.items()]

    #                 for item in response:
    #                     item['available_qty'] = int(item.get('available_qty', 0))
    #                 max_available_qty_by_mfr = {}
    #                 for item in response:
    #                     mfr_part_number = item['mfr_part_number']
    #                     if mfr_part_number not in max_available_qty_by_mfr or item['available_qty'] > \
    #                             max_available_qty_by_mfr[mfr_part_number]['available_qty']:
    #                         max_available_qty_by_mfr[mfr_part_number] = item
    #                 result = list(max_available_qty_by_mfr.values())
    #                 return {"statusCode": 200, "body": result}
    #                 # return {"statusCode": 200, "body": response}
    #             elif status_type == "BOX BUILDING":
    #                 stock_details_list = [item['all_attributes']['available_stock']['M-Kit'] for item in query]
    #                 mfr_part_numbers = []
    #                 for item in stock_details_list:
    #                     for part in item.values():
    #                         mfr_part_numbers.append(part.get('cmpt_id', None))
    #                 result = []
    #                 for i in total_price:
    #                     if "all_attributes" in i and "parts" in i["all_attributes"]:
    #                         parts = i["all_attributes"]["parts"]
    #                         res = {}
    #                         for part_key in parts:
    #                             if part_key.startswith('part'):
    #                                 if "mfr_prt_num" in i["all_attributes"]["parts"][part_key]:
    #                                     if i["all_attributes"]["parts"][part_key]['mfr_prt_num'] in mfr_part_numbers:
    #                                         res['unit'] = i["all_attributes"]["parts"][part_key]["unit_price"]
    #                                         res['timestamp'] = i["sk_timeStamp"]
    #                                         res['mfr_prt_num'] = i["all_attributes"]["parts"][part_key]['mfr_prt_num']
    #                                         result.append(res)
    #                 # print(result)
    #                 max_units = {}
    #                 for item in result:
    #                     mfr_prt_num = item['mfr_prt_num']
    #                     unit = item['unit']
    #                     if mfr_prt_num not in max_units or int(unit) > int(max_units[mfr_prt_num]):
    #                         max_units[mfr_prt_num] = unit
    #                 for parts in stock_details_list:
    #                     for _, part in parts.items():
    #                         mfr_part_number = part['mfr_part_number']
    #                         part['unit_price'] = max_units.get(mfr_part_number, '-')
    #                 response = [part for parts in stock_details_list for key, part in parts.items()]
    #                 for item in response:
    #                     item['available_qty'] = int(item.get('available_qty', 0))
    #                 max_available_qty_by_mfr = {}
    #                 for item in response:
    #                     mfr_part_number = item['mfr_part_number']
    #                     if mfr_part_number not in max_available_qty_by_mfr or item['available_qty'] > \
    #                             max_available_qty_by_mfr[mfr_part_number]['available_qty']:
    #                         max_available_qty_by_mfr[mfr_part_number] = item
    #                 result = list(max_available_qty_by_mfr.values())
    #                 return {"statusCode": 200, "body": result}

    #                 # print(response)
    #                 return {"statusCode": 200, "body": response}
    #             else:
    #                 return {"statusCode": 400, "body": "Invalid status type"}
    #         else:
    #             return {"statusCode": 200, "body": stock_details_list}
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         error_message = f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}"
    #         print(error_message)
    #         return {'statusCode': 400, 'body': []}

    # def cmsPartnerEMSUpdateStockFetch(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         partner_id = data['partner_id']
    #         filter_bom_id = data['bom_id']
    #         filter = {
    #             'all_attributes.partner_id': partner_id,
    #             'all_attributes.bom_id': filter_bom_id
    #         }
    #         ems = list(db_con.EMS.find(filter))
    #         partner = list(db_con.Partners.find({'all_attributes.partner_id': partner_id}))
    #         ems_cmpt = {}
    #         partner_cmpt = {}
    #         for i in ems:
    #             all_attributes = i.get('all_attributes', {})
    #             for key, value in all_attributes.items():
    #                 if key.startswith('E-KIT'):
    #                     for k, v in value.items():
    #                         cmpt_id = v.get('cmpt_id')
    #                         provided_qty = v.get('provided_qty')
    #                         if cmpt_id and provided_qty is not None:
    #                             if cmpt_id in ems_cmpt:
    #                                 ems_cmpt[cmpt_id] += int(provided_qty)
    #                             else:
    #                                 ems_cmpt[cmpt_id] = int(provided_qty)
    #         for i in partner:
    #             all_attributes = i.get('all_attributes', {})
    #             available_stock = all_attributes.get('available_stock', {})
    #             for kit, parts in available_stock.items():
    #                 for part, details in parts.items():
    #                     cmpt_id = details.get('cmpt_id')
    #                     utilized_qty = int(details.get('utilized_qty', 0))
    #                     damaged_qty = int(details.get('damaged_qty', 0))
    #                     if cmpt_id:
    #                         partner_cmpt[cmpt_id] = utilized_qty + damaged_qty
    #         ekit_data = list(db_con.EMS.find({'all_attributes.partner_id': partner_id}))
    #         bom_ids = [item['all_attributes']['bom_id'] for item in ekit_data]
    #         bom_constraints = list(db_con.BOM.find({'pk_id': {'$in': bom_ids}}))
    #         bom_id_list = [item['pk_id'] for item in bom_constraints]
    #         parts = []
    #         all_mfr_prt_nums = set()
    #         bom_ids_to_process = bom_id_list if filter_bom_id == 'All' else [filter_bom_id]
    #         for bom_id in bom_ids_to_process:
    #             query = list(db_con.EMS.find({'all_attributes.bom_id': bom_id}))
    #             res = [{key: value for key, value in i.get('all_attributes', {}).items() if key.startswith('E-KIT')} for
    #                    i in query]
    #             all_mfr_prt_nums.update(
    #                 [part_value.get('mfr_part_number') for entry in res for kit, parts in entry.items() for part, part_value
    #                  in parts.items()]
    #             )
    #         inventory_items = list(db_con.Inventory.find({'mfr_prt_num': {'$in': list(all_mfr_prt_nums)}},
    #                                                      {'mfr_prt_num': 1, 'qty': 1}))
    #         inventory_qty_map = {item.get('mfr_prt_num'): item.get('qty', 0) for item in inventory_items}
    #         part_counter = 1
    #         for bom_id in bom_ids_to_process:
    #             query = list(db_con.EMS.find({'all_attributes.bom_id': bom_id}))
    #             res = [{key: value for key, value in i.get('all_attributes', {}).items() if key.startswith('E-KIT')} for
    #                    i in query]
    #             parts_list = [value for dictionary in res for key, value in dictionary.items()]
    #             for part in parts_list:
    #                 for part_key, part_value in part.items():
    #                     part_value['utilized_qty'] = '0'
    #                     part_value['damaged_qty'] = part_value.get('damaged_qty', '0')
    #                     cmpt_id = part_value.get('cmpt_id')
    #                     part_value['available_qty'] = f"{ems_cmpt.get(cmpt_id, 0) - partner_cmpt.get(cmpt_id, 0)}"
    #                     part_value['bom_id'] = bom_id if filter_bom_id == 'All' else None
    #                     parts.append({f"part{part_counter}": part_value})
    #                     part_counter += 1
    #         final_parts_dict = {}
    #         for item in parts:
    #             for part_key, part_value in item.items():
    #                 mfr_part_number = part_value.get('mfr_part_number')
    #                 if mfr_part_number not in final_parts_dict:
    #                     final_parts_dict[mfr_part_number] = part_value
    #                 else:
    #                     current = final_parts_dict[mfr_part_number]
    #                     current['provided_qty'] = max(current.get('provided_qty', 0), part_value.get('provided_qty', 0))
    #                     current['utilized_qty'] = max(current.get('utilized_qty', 0), part_value.get('utilized_qty', 0))
    #                     current['damaged_qty'] = max(current.get('damaged_qty', 0), part_value.get('damaged_qty', 0))
    #                     current['available_qty'] = max(current.get('available_qty', 0),
    #                                                    part_value.get('available_qty', 0))
    #                     final_parts_dict[mfr_part_number] = current
    #         final_parts = [{key: value for key, value in part_value.items() if key != 'sno'} for part_value in
    #                        final_parts_dict.values()]
    #         final_parts = [
    #             part for part in final_parts
    #             if 'ptg_prt_num' in part
    #         ]
    #         final_parts_updated = [
    #             {key: value for key, value in part.items() if key != 'sno'}
    #             for part in final_parts
    #         ]
    #         return {'statusCode': 200, 'body': final_parts_updated}
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         return {'statusCode': 400, 'body': f'Unable to fetch data: {err}, {f_name}, {line_no}'}

    # def cmsPartnerGetStock(request_body):
    #     try:
    #         data = request_body
    #         env_type = data.get('env_type')
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         client = db_conct['client']
    #         partner_id = data['partner_id']
    #         status_type = data['status']
    #         query = list(db_con.Partners.find({'pk_id': partner_id}, {'_id': 0}))
    #         total_price = list(db_con.PurchaseOrder.find())
    #         stock_details_list = []
    #         if query:
    #             if status_type == "EMS":
    #                 for item in query:
    #                     if 'E-Kit' in item['all_attributes']['available_stock']:
    #                         available_stock = item['all_attributes']['available_stock']['E-Kit']
    #                         for bom_id, bom_parts in available_stock.items():
    #                             for part_key, part_value in bom_parts.items():
    #                                 part_value['bom_id'] = bom_id  # Add bom_id to the part details
    #                                 stock_details_list.append(part_value)
    #                 mfr_part_numbers = []
    #                 for part in stock_details_list:
    #                     mfr_part_numbers.append(part.get('mfr_part_number', None))
    #                 result = []
    #                 for i in total_price:
    #                     if "all_attributes" in i and "parts" in i["all_attributes"]:
    #                         parts = i["all_attributes"]["parts"]
    #                         res = {}
    #                         for part_key in parts:
    #                             if part_key.startswith('part'):
    #                                 if "mfr_prt_num" in i["all_attributes"]["parts"][part_key]:
    #                                     if i["all_attributes"]["parts"][part_key]['mfr_prt_num'] in mfr_part_numbers:
    #                                         res['unit'] = i["all_attributes"]["parts"][part_key]["unit_price"]
    #                                         res['timestamp'] = i["sk_timeStamp"]
    #                                         res['mfr_prt_num'] = i["all_attributes"]["parts"][part_key]['mfr_prt_num']
    #                                         result.append(res)
    #                 max_units = {}
    #                 for item in result:
    #                     mfr_prt_num = item['mfr_prt_num']
    #                     unit = item['unit']
    #                     if isinstance(unit, str):
    #                         try:
    #                             unit = float(unit)
    #                         except ValueError:
    #                             unit = 0  # or handle the error as needed
    #                     if mfr_prt_num not in max_units or float(unit) > float(max_units[mfr_prt_num]):
    #                         max_units[mfr_prt_num] = unit
    #                 for part in stock_details_list:
    #                     part['unit_price'] = max_units.get(part['mfr_part_number'], '-')
    #                 return {"statusCode": 200, "body": stock_details_list}
    #             elif status_type == "BOX BUILDING":
    #                 stock_details_list = [item['all_attributes']['available_stock']['M-Kit'] for item in query]
    #                 mfr_part_numbers = []
    #                 for item in stock_details_list:
    #                     for part in item.values():
    #                         mfr_part_numbers.append(part.get('cmpt_id', None))
    #                 result = []
    #                 for i in total_price:
    #                     if "all_attributes" in i and "parts" in i["all_attributes"]:
    #                         parts = i["all_attributes"]["parts"]
    #                         res = {}
    #                         for part_key in parts:
    #                             if part_key.startswith('part'):
    #                                 if "mfr_prt_num" in i["all_attributes"]["parts"][part_key]:
    #                                     if i["all_attributes"]["parts"][part_key]['mfr_prt_num'] in mfr_part_numbers:
    #                                         res['unit'] = i["all_attributes"]["parts"][part_key]["unit_price"]
    #                                         res['timestamp'] = i["sk_timeStamp"]
    #                                         res['mfr_prt_num'] = i["all_attributes"]["parts"][part_key]['mfr_prt_num']
    #                                         result.append(res)
    #                 max_units = {}
    #                 for item in result:
    #                     mfr_prt_num = item['mfr_prt_num']
    #                     unit = item['unit']
    #                     if mfr_prt_num not in max_units or int(unit) > int(max_units[mfr_prt_num]):
    #                         max_units[mfr_prt_num] = unit
    #                 for parts in stock_details_list:
    #                     for _, part in parts.items():
    #                         mfr_part_number = part['mfr_part_number']
    #                         part['unit_price'] = max_units.get(mfr_part_number, '-')
    #                 response = [part for parts in stock_details_list for key, part in parts.items()]
    #                 for item in response:
    #                     item['available_qty'] = int(item.get('available_qty', 0))
    #                 max_available_qty_by_mfr = {}
    #                 for item in response:
    #                     mfr_part_number = item['mfr_part_number']
    #                     if mfr_part_number not in max_available_qty_by_mfr or item['available_qty'] > \
    #                             max_available_qty_by_mfr[mfr_part_number]['available_qty']:
    #                         max_available_qty_by_mfr[mfr_part_number] = item
    #                 result = list(max_available_qty_by_mfr.values())
    #                 return {"statusCode": 200, "body": result}

    #                 # print(response)
    #                 return {"statusCode": 200, "body": response}
    #             else:
    #                 return {"statusCode": 400, "body": "Invalid status type"}
    #         else:
    #             return {"statusCode": 200, "body": stock_details_list}
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         error_message = f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}"
    #         print(error_message)
    #         return {'statusCode': 400, 'body': []}

    def cmsPartnerGetStock(request_body):
        try:
            data = request_body
            env_type = data.get('env_type')
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            partner_id = data['partner_id']
            status_type = data['status']
            query = list(db_con.Partners.find({'pk_id': partner_id}, {'_id': 0}))
            total_price = list(db_con.PurchaseOrder.find())
            stock_details_list = []
            if query:
                if status_type == "EMS":
                    for item in query:
                        if 'E-Kit' in item['all_attributes']['available_stock']:
                            available_stock = item['all_attributes']['available_stock']['E-Kit']
                            for bom_id, bom_parts in available_stock.items():
                                for part_key, part_value in bom_parts.items():
                                    part_value['bom_id'] = bom_id  # Add bom_id to the part details
                                    stock_details_list.append(part_value)
                    mfr_part_numbers = []
                    for part in stock_details_list:
                        mfr_part_numbers.append(part.get('mfr_part_number', None))
                    result = []
                    for i in total_price:
                        if "all_attributes" in i and "parts" in i["all_attributes"]:
                            parts = i["all_attributes"]["parts"]
                            res = {}
                            for part_key in parts:
                                if part_key.startswith('part'):
                                    if "mfr_prt_num" in i["all_attributes"]["parts"][part_key]:
                                        if i["all_attributes"]["parts"][part_key]['mfr_prt_num'] in mfr_part_numbers:
                                            res['unit'] = i["all_attributes"]["parts"][part_key]["unit_price"]
                                            res['timestamp'] = i["sk_timeStamp"]
                                            res['mfr_prt_num'] = i["all_attributes"]["parts"][part_key]['mfr_prt_num']
                                            result.append(res)
                    max_units = {}
                    for item in result:
                        mfr_prt_num = item['mfr_prt_num']
                        unit = item['unit']
                        if isinstance(unit, str):
                            try:
                                unit = float(unit)
                            except ValueError:
                                unit = 0  # or handle the error as needed
                        if mfr_prt_num not in max_units or float(unit) > float(max_units[mfr_prt_num]):
                            max_units[mfr_prt_num] = unit
                    for part in stock_details_list:
                        part['unit_price'] = max_units.get(part['mfr_part_number'], '-')
                    aggregated_data = {}
                    for item in stock_details_list:
                        key = (item['mfr_part_number'],
                               item['ptg_prt_num'])
                        if key not in aggregated_data:
                            aggregated_data[
                                key] = item.copy()
                            aggregated_data[key]['available_qty'] = int(aggregated_data[key]['available_qty'])
                            aggregated_data[key]['utilized_qty'] = int(aggregated_data[key]['utilized_qty'])
                            aggregated_data[key]['damaged_qty'] = int(aggregated_data[key]['damaged_qty'])
                        else:
                            aggregated_data[key]['available_qty'] += int(item['available_qty'])
                            aggregated_data[key]['utilized_qty'] += int(item['utilized_qty'])
                            aggregated_data[key]['damaged_qty'] += int(item['damaged_qty'])
                    aggregated_list = list(aggregated_data.values())
                    return {"statusCode": 200, "body": aggregated_list}
                elif status_type == "BOX BUILDING":
                    for item in query:
                        if 'M-Kit' in item['all_attributes']['available_stock']:
                            available_stock = item['all_attributes']['available_stock']['M-Kit']
                            for bom_id, bom_parts in available_stock.items():
                                for part_key, part_value in bom_parts.items():
                                    part_value['bom_id'] = bom_id  # Add bom_id to the part details
                                    stock_details_list.append(part_value)
                    mfr_part_numbers = []
                    for part in stock_details_list:
                        mfr_part_numbers.append(part.get('mfr_part_number', None))
                    result = []
                    for i in total_price:
                        if "all_attributes" in i and "parts" in i["all_attributes"]:
                            parts = i["all_attributes"]["parts"]
                            res = {}
                            for part_key in parts:
                                if part_key.startswith('part'):
                                    if "mfr_prt_num" in i["all_attributes"]["parts"][part_key]:
                                        if i["all_attributes"]["parts"][part_key]['mfr_prt_num'] in mfr_part_numbers:
                                            res['unit'] = i["all_attributes"]["parts"][part_key]["unit_price"]
                                            res['timestamp'] = i["sk_timeStamp"]
                                            res['mfr_prt_num'] = i["all_attributes"]["parts"][part_key]['mfr_prt_num']
                                            result.append(res)
                    max_units = {}
                    for item in result:
                        mfr_prt_num = item['mfr_prt_num']
                        unit = item['unit']
                        if isinstance(unit, str):
                            try:
                                unit = float(unit)
                            except ValueError:
                                unit = 0  # or handle the error as needed
                        if mfr_prt_num not in max_units or float(unit) > float(max_units[mfr_prt_num]):
                            max_units[mfr_prt_num] = unit
                    for part in stock_details_list:
                        part['unit_price'] = max_units.get(part['mfr_part_number'], '-')
                    aggregated_data = {}
                    for item in stock_details_list:
                        key = (item['mfr_part_number'], item['part_name'],
                               item['cmpt_id'])
                        if key not in aggregated_data:
                            aggregated_data[
                                key] = item.copy()
                            aggregated_data[key]['available_qty'] = int(aggregated_data[key]['available_qty'])
                            aggregated_data[key]['utilized_qty'] = int(aggregated_data[key]['utilized_qty'])
                            aggregated_data[key]['damaged_qty'] = int(aggregated_data[key]['damaged_qty'])
                        else:
                            aggregated_data[key]['available_qty'] += int(item['available_qty'])
                            aggregated_data[key]['utilized_qty'] += int(item['utilized_qty'])
                            aggregated_data[key]['damaged_qty'] += int(item['damaged_qty'])
                    aggregated_list = list(aggregated_data.values())
                    return {"statusCode": 200, "body": aggregated_list}
                else:
                    return {"statusCode": 400, "body": "Invalid status type"}
            else:
                return {"statusCode": 200, "body": stock_details_list}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            error_message = f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}"
            print(error_message)
            return {'statusCode': 400, 'body': []}

    def cmsPartnerEMSUpdateStockFetch(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            partner_id = data['partner_id']
            filter_bom_id = data['bom_id']
            filter = {
                'all_attributes.partner_id': partner_id,
                'all_attributes.bom_id': filter_bom_id
            }
            ems = list(db_con.EMS.find(filter))
            partner = db_con.Partners.find_one({'all_attributes.partner_id': partner_id},{'all_attributes':1})
            if not partner:
                return {'statusCode': 400, 'body': 'Partner not found'}
            available_stock = partner.get('all_attributes', {}).get('available_stock', {})
            ems_cmpt = {}
            partner_cmpt = {}
            for bom_id, parts in available_stock.get("E-Kit", {}).items():
                if filter_bom_id in available_stock.get("E-Kit", {}):
                    # for bom_id, parts in available_stock.get("E-Kit", {}).items():
                    if filter_bom_id == 'All' or bom_id == filter_bom_id:
                        for part_key, part_data in parts.items():
                            cmpt_id = part_data.get('cmpt_id')
                            provided_qty = part_data.get('provided_qty')
                            utilized_qty = int(part_data.get('utilized_qty', 0))
                            damaged_qty = int(part_data.get('damaged_qty', 0))
                            if cmpt_id and provided_qty is not None:
                                if cmpt_id in ems_cmpt:
                                    ems_cmpt[cmpt_id] += int(provided_qty)
                                else:
                                    ems_cmpt[cmpt_id] = int(provided_qty)
                            if cmpt_id:
                                partner_cmpt[cmpt_id] = utilized_qty + damaged_qty
                        ekit_data = list(db_con.EMS.find({'all_attributes.partner_id': partner_id}))
                        bom_ids = [item['all_attributes']['bom_id'] for item in ekit_data]
                        bom_constraints = list(db_con.BOM.find({'pk_id': {'$in': bom_ids}}))
                        bom_id_list = [item['pk_id'] for item in bom_constraints]
                        parts = []
                        all_mfr_prt_nums = set()
                        bom_ids_to_process = bom_id_list if filter_bom_id == 'All' else [filter_bom_id]

                        for bom_id in bom_ids_to_process:
                            query = list(db_con.EMS.find({'all_attributes.bom_id': bom_id}))
                            res = [{key: value for key, value in i.get('all_attributes', {}).items() if key.startswith('E-KIT')} for
                                   i in query]
                            all_mfr_prt_nums.update(
                                [part_value.get('mfr_part_number') for entry in res for kit, parts in entry.items() for
                                 part, part_value in parts.items()]
                            )

                        inventory_items = list(
                            db_con.Inventory.find({'mfr_prt_num': {'$in': list(all_mfr_prt_nums)}}, {'mfr_prt_num': 1, 'qty': 1}))
                        inventory_qty_map = {item.get('mfr_prt_num'): item.get('qty', 0) for item in inventory_items}

                        part_counter = 1
                        for bom_id in bom_ids_to_process:
                            query = list(db_con.EMS.find({'all_attributes.bom_id': bom_id}))
                            res = [{key: value for key, value in i.get('all_attributes', {}).items() if key.startswith('E-KIT')} for
                                   i in query]
                            parts_list = [value for dictionary in res for key, value in dictionary.items()]

                            for part in parts_list:
                                for part_key, part_value in part.items():
                                    cmpt_id = part_value.get('cmpt_id')
                                    utilized_qty = int(part_value.get('utilized_qty', 0))
                                    damaged_qty = int(part_value.get('damaged_qty', 0))
                                    provided_qty = int(part_value.get('provided_qty', 0))
                                    available_qty = ems_cmpt.get(cmpt_id, 0) - partner_cmpt.get(cmpt_id, 0)

                                    part_value['available_qty'] = max(0, available_qty)
                                    part_value['bom_id'] = bom_id if filter_bom_id == 'All' else None
                                    part_value['utilized_qty'] = utilized_qty
                                    part_value['damaged_qty'] = damaged_qty
                                    part_value['provided_qty'] = provided_qty

                                    parts.append({f"part{part_counter}": part_value})
                                    part_counter += 1

                        final_parts_dict = {}
                        for item in parts:
                            for part_key, part_value in item.items():
                                mfr_part_number = part_value.get('mfr_part_number')
                                if mfr_part_number not in final_parts_dict:
                                    final_parts_dict[mfr_part_number] = part_value
                                else:
                                    current = final_parts_dict[mfr_part_number]
                                    current['provided_qty'] = max(current.get('provided_qty', 0), part_value.get('provided_qty', 0))
                                    current['utilized_qty'] = max(current.get('utilized_qty', 0), part_value.get('utilized_qty', 0))
                                    current['damaged_qty'] = max(current.get('damaged_qty', 0), part_value.get('damaged_qty', 0))
                                    current['available_qty'] = max(current.get('available_qty', 0),
                                                                   part_value.get('available_qty', 0))
                                    final_parts_dict[mfr_part_number] = current

                        final_parts = [{key: value for key, value in part_value.items() if key != 'sno'} for part_value in
                                       final_parts_dict.values()]
                        final_parts_updated = [{key: value for key, value in part.items() if key != 'sno'} for part in final_parts]

                        final_parts_updated = [{**i, 'bom_id': data['bom_id']} if 'bom_id' in i else i for i in final_parts_updated]
                        response = [i for i in final_parts_updated if 'mfr_part_number' in i and 'manufacturer' in i]
                        return {'statusCode': 200, 'body': response}
                        # return {'statusCode': 200, 'body': final_parts_updated}
                # else:
            for i in ems:
                all_attributes = i.get('all_attributes', {})
                for key, value in all_attributes.items():
                    if key.startswith('E-KIT'):
                        for k, v in value.items():
                            cmpt_id = v.get('cmpt_id')
                            provided_qty = v.get('provided_qty')
                            if cmpt_id and provided_qty is not None:
                                if cmpt_id in ems_cmpt:
                                    ems_cmpt[cmpt_id] += int(provided_qty)
                                else:
                                    ems_cmpt[cmpt_id] = int(provided_qty)
            # for i in partner:
            all_attributes = partner.get('all_attributes', {})
            available_stock = all_attributes.get('available_stock', {})
            for kit, parts in available_stock.items():
                for part, details in parts.items():
                    cmpt_id = details.get('cmpt_id')
                    utilized_qty = int(details.get('utilized_qty', 0))
                    damaged_qty = int(details.get('damaged_qty', 0))
                    if cmpt_id:
                        partner_cmpt[cmpt_id] = utilized_qty + damaged_qty
            ekit_data = list(db_con.EMS.find({'all_attributes.partner_id': partner_id}))
            bom_ids = [item['all_attributes']['bom_id'] for item in ekit_data]
            bom_constraints = list(db_con.BOM.find({'pk_id': {'$in': bom_ids}}))
            bom_id_list = [item['pk_id'] for item in bom_constraints]
            parts = []
            all_mfr_prt_nums = set()
            bom_ids_to_process = bom_id_list if filter_bom_id == 'All' else [filter_bom_id]
            for bom_id in bom_ids_to_process:
                query = list(db_con.EMS.find({'all_attributes.bom_id': bom_id}))
                res = [{key: value for key, value in i.get('all_attributes', {}).items() if
                        key.startswith('E-KIT')} for
                       i in query]
                all_mfr_prt_nums.update(
                    [part_value.get('mfr_part_number') for entry in res for kit, parts in entry.items() for
                     part, part_value
                     in parts.items()]
                )
            inventory_items = list(db_con.Inventory.find({'mfr_prt_num': {'$in': list(all_mfr_prt_nums)}},
                                                         {'mfr_prt_num': 1, 'qty': 1}))
            inventory_qty_map = {item.get('mfr_prt_num'): item.get('qty', 0) for item in inventory_items}
            part_counter = 1
            for bom_id in bom_ids_to_process:
                query = list(db_con.EMS.find({'all_attributes.bom_id': bom_id}))
                res = [{key: value for key, value in i.get('all_attributes', {}).items() if
                        key.startswith('E-KIT')} for
                       i in query]
                parts_list = [value for dictionary in res for key, value in dictionary.items()]
                for part in parts_list:
                    for part_key, part_value in part.items():
                        part_value['utilized_qty'] = '0'
                        part_value['damaged_qty'] = part_value.get('damaged_qty', '0')
                        cmpt_id = part_value.get('cmpt_id')
                        part_value[
                            'available_qty'] = f"{ems_cmpt.get(cmpt_id, 0) - partner_cmpt.get(cmpt_id, 0)}"
                        part_value['bom_id'] = bom_id if filter_bom_id == 'All' else None
                        parts.append({f"part{part_counter}": part_value})
                        part_counter += 1
            final_parts_dict = {}
            for item in parts:
                for part_key, part_value in item.items():
                    mfr_part_number = part_value.get('mfr_part_number')
                    if mfr_part_number not in final_parts_dict:
                        final_parts_dict[mfr_part_number] = part_value
                    else:
                        current = final_parts_dict[mfr_part_number]
                        current['provided_qty'] = max(current.get('provided_qty', 0),
                                                      part_value.get('provided_qty', 0))
                        current['utilized_qty'] = max(current.get('utilized_qty', 0),
                                                      part_value.get('utilized_qty', 0))
                        current['damaged_qty'] = max(current.get('damaged_qty', 0),
                                                     part_value.get('damaged_qty', 0))
                        current['available_qty'] = max(current.get('available_qty', 0),
                                                       part_value.get('available_qty', 0))
                        final_parts_dict[mfr_part_number] = current
            final_parts = [{key: value for key, value in part_value.items() if key != 'sno'} for part_value in
                           final_parts_dict.values()]
            final_parts = [
                part for part in final_parts
                if 'ptg_prt_num' in part
            ]
            final_parts_updated = [
                {key: value for key, value in part.items() if key != 'sno'}
                for part in final_parts
            ]
            final_parts_updated = [{**i, 'bom_id': data['bom_id']} if 'bom_id' in i else i for i in
                                   final_parts_updated]
            return {'statusCode': 200, 'body': final_parts_updated}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400, 'body': f'Unable to fetch data: {err}, {f_name}, {line_no}'}

    # def cmsPartnerEMSUpdateStockSaveComponents(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         partner_id = data['partner_id']
    #         updates = data['update_stock']
    #         partner = db_con.Partners.find_one_and_update(
    #             {"pk_id": partner_id},
    #             {"$setOnInsert": {"all_attributes.available_stock.E-Kit": {}}},
    #             upsert=True,
    #             return_document=True
    #         )
    #         available_stock = partner.get("all_attributes", {}).get("available_stock", {}).get("E-Kit", {})
    #         existing_cmpt_ids = {details.get("cmpt_id"): key for key, details in available_stock.items()}
    #         combined_list = []
    #         next_key_index = len(existing_cmpt_ids) + 1
    #         for item in updates:
    #             cmpt_id = str(item.get("cmpt_id"))
    #             new_utilized_qty = int(item.get("utilized_qty", 0))
    #             new_damaged_qty = int(item.get("damaged_qty", 0))
    #             total_new_qty = new_utilized_qty + new_damaged_qty
    #             if cmpt_id in existing_cmpt_ids:
    #                 part_key = existing_cmpt_ids[cmpt_id]
    #                 existing_item = available_stock[part_key]
    #                 existing_utilized_qty = int(existing_item.get("utilized_qty", 0))
    #                 existing_damaged_qty = int(existing_item.get("damaged_qty", 0))
    #                 existing_available_qty = int(existing_item.get("available_qty", 0))
    #                 updated_utilized_qty = existing_utilized_qty + new_utilized_qty
    #                 updated_damaged_qty = existing_damaged_qty + new_damaged_qty
    #                 updated_available_qty = existing_available_qty - total_new_qty
    #                 if updated_available_qty < 0:
    #                     return {'statusCode': 400,
    #                             'body': 'utilized_qty and damaged_qty should not exceed available_qty'}
    #                 item.update({
    #                     "utilized_qty": updated_utilized_qty,
    #                     "damaged_qty": updated_damaged_qty,
    #                     "available_qty": updated_available_qty
    #                 })
    #                 combined_list.append((part_key, item))
    #             else:
    #                 initial_available_qty = int(item.get("available_qty", 0))
    #                 new_available_qty = initial_available_qty - total_new_qty
    #                 if new_available_qty < 0:
    #                     return {'statusCode': 400,
    #                             'body': 'utilized_qty and damaged_qty should not exceed available_qty'}
    #                 new_key = f"part{next_key_index}"
    #                 item["available_qty"] = new_available_qty
    #                 combined_list.append((new_key, item))
    #                 next_key_index += 1
    #         for part_key, item in combined_list:
    #             update_fields = {
    #                 f"all_attributes.available_stock.E-Kit.{part_key}.{k}": str(v) if isinstance(v, int) else v for k, v
    #                 in item.items()}
    #             db_con.Partners.update_one({"pk_id": partner_id}, {"$set": update_fields}, upsert=True)
    #         return {'statusCode': 200, 'body': 'Records updated and inserted successfully'}
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400, 'body': 'Updating or inserting components of stock in Partners failed'}

    def cmsPartnerEMSUpdateStockSaveComponents(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            partner_id = data['partner_id']
            updates = data['update_stock']
            partner = db_con.Partners.find_one_and_update(
                {"pk_id": partner_id},
                {"$setOnInsert": {"all_attributes.available_stock.E-Kit": {}}},
                upsert=True,
                return_document=True
            )
            available_stock = partner.get("all_attributes", {}).get("available_stock", {}).get("E-Kit", {}).get(data['bom_id'], {})
            existing_cmpt_ids = {details.get("cmpt_id"): key for key, details in available_stock.items()}
            combined_list = []
            next_key_index = 1
            for item in updates:
                cmpt_id = str(item.get("cmpt_id"))
                new_utilized_qty = int(item.get("utilized_qty", 0))
                new_damaged_qty = int(item.get("damaged_qty", 0))
                total_new_qty = new_utilized_qty + new_damaged_qty
                if cmpt_id in existing_cmpt_ids:
                    part_key = existing_cmpt_ids[cmpt_id]
                    existing_item = available_stock[part_key]
                    existing_utilized_qty = int(existing_item.get("utilized_qty", 0))
                    existing_damaged_qty = int(existing_item.get("damaged_qty", 0))
                    existing_available_qty = int(existing_item.get("available_qty", 0))
                    updated_utilized_qty = existing_utilized_qty + new_utilized_qty
                    updated_damaged_qty = existing_damaged_qty + new_damaged_qty
                    updated_available_qty = existing_available_qty - total_new_qty
                    if updated_available_qty < 0:
                        return {'statusCode': 400,
                                'body': 'utilized_qty and damaged_qty should not exceed available_qty'}
                    item.update({
                        "utilized_qty": updated_utilized_qty,
                        "damaged_qty": updated_damaged_qty,
                        "available_qty": updated_available_qty
                    })
                    combined_list.append((part_key, item))
                else:
                    initial_available_qty = int(item.get("available_qty", 0))
                    new_available_qty = initial_available_qty - total_new_qty
                    if new_available_qty < 0:
                        return {'statusCode': 400,
                                'body': 'utilized_qty and damaged_qty should not exceed available_qty'}
                    new_key = f"part{next_key_index}"
                    item["available_qty"] = new_available_qty
                    combined_list.append((new_key, item))
                    next_key_index += 1
            for part_key, item in combined_list:
                update_fields = {
                    f"all_attributes.available_stock.E-Kit.{data['bom_id']}.{part_key}.{k}": str(v) if isinstance(v, int) else v for k, v
                    in item.items()}
                db_con.Partners.update_one({"pk_id": partner_id}, {"$set": update_fields}, upsert=True)
            return {'statusCode': 200, 'body': 'Records updated and inserted successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Updating or inserting components of stock in Partners failed'}
        
    def cmsPartnerUpdateStockBOMList(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            partner_id = data['partner_id']
            status = data['status']
            all_boms = []
            if status == 'EMS':
                filter = list(db_con.EMS.find({'all_attributes.partner_id': partner_id}))
                ems_bom_id = [item['all_attributes']['bom_id'] for item in filter]
                query = list(db_con.BOM.find({'pk_id': {'$in': ems_bom_id}}))
                ems_bom_list = [{'bom_id': item['pk_id'], 'bom_name': item['all_attributes']['bom_name']} for item in query]
                return {'statusCode': 200, 'body': ems_bom_list}
            elif status == 'BoxBuilding':
                filter = list(db_con.BoxBuilding.find({'all_attributes.partner_id': partner_id}))
                bb_bom_id = [item['all_attributes']['bom_id'] for item in filter]
                query = list(db_con.BOM.find({'pk_id': {'$in': bb_bom_id}}))
                bb_bom_list = [{'bom_id': item['pk_id'], 'bom_name': item['all_attributes']['bom_name']} for item in query]
                return {'statusCode': 200, 'body': bb_bom_list}
            else:
                ems_filter = list(db_con.EMS.find({'all_attributes.partner_id': partner_id}))
                bb_filter = list(db_con.BoxBuilding.find({'all_attributes.partner_id': partner_id}))
                ems_bom_id = [item['all_attributes']['bom_id'] for item in ems_filter]
                bb_bom_id = [item['all_attributes']['bom_id'] for item in bb_filter]
                ems_query = list(db_con.BOM.find({'pk_id': {'$in': ems_bom_id}}))
                bb_query = list(db_con.BOM.find({'pk_id': {'$in': bb_bom_id}}))
                ems_bom_list = [{'bom_id': item['pk_id'], 'bom_name': item['all_attributes']['bom_name']} for item in ems_query]
                bb_bom_list = [{'bom_id': item['pk_id'], 'bom_name': item['all_attributes']['bom_name']} for item in bb_query]
                all_boms.extend(ems_bom_list)
                all_boms.extend(bb_bom_list)
                return {'statusCode': 200, 'body': all_boms}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400, 'body': 'Unable to Fetch bom ids and bom names'}

    # def cmsPartnerBBUpdateStockSaveComponents(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         partner_id = data['partner_id']
    #         update = data['update_stock']
    #         partner = db_con.Partners.find_one({"pk_id": partner_id})
    #         if not partner:
    #             partner = {"pk_id": partner_id, "all_attributes": {"available_stock": {"M-Kit": {}}}}
    #             db_con.Partners.insert_one(partner)
    #         available_stock = partner.get("all_attributes", {}).get("available_stock", {}).get("M-Kit", {})
    #         existing_mfr_part_numbers = {details.get("mfr_part_number"): key for key, details in
    #                                      available_stock.items()}
    #         existing_part_keys = set(available_stock.keys())
    #         combined_list = []
    #         next_key_index = max([int(key.replace('part', '')) for key in existing_part_keys], default=0) + 1
    #         for item in update:
    #             mfr_part_number = item.get("mfr_part_number")
    #             utilized_qty = str(item.get("utilized_qty", 0))
    #             damaged_qty = str(item.get("damaged_qty", 0))
    #             combined_qty = int(utilized_qty) + int(damaged_qty)
    #             current_qty = str(int(item.get("available_qty", 0)) - combined_qty)
    #             if mfr_part_number in existing_mfr_part_numbers:
    #                 part_key = existing_mfr_part_numbers[mfr_part_number]
    #                 existing_qty = int(available_stock[part_key].get("available_qty", 0))
    #                 new_qty = existing_qty - combined_qty
    #                 if new_qty < 0:
    #                     return {'statusCode': 400,
    #                             'body': f'utilized_qty and damaged_qty should not exceed available_qty'}
    #                 item["available_qty"] = str(new_qty)
    #                 combined_list.append((part_key, item))
    #             else:
    #                 if int(current_qty) < 0:
    #                     return {'statusCode': 400,
    #                             'body': f'utilized_qty and damaged_qty should not exceed available_qty'}
    #                 item["available_qty"] = current_qty
    #                 new_key = f"part{next_key_index}"
    #                 combined_list.append((new_key, item))
    #                 next_key_index += 1
    #         updates = []
    #         for part_key, item in combined_list:
    #             for k, v in item.items():
    #                 if k in ["utilized_qty", "damaged_qty"]:
    #                     current_value = int(available_stock.get(part_key, {}).get(k, 0))
    #                     updates.append({"pk_id": partner_id,
    #                                     f"all_attributes.available_stock.M-Kit.{part_key}.{k}": str(
    #                                         current_value + int(v))})
    #                 else:
    #                     updates.append({"pk_id": partner_id, f"all_attributes.available_stock.M-Kit.{part_key}.{k}": v})
    #         for update in updates:
    #             db_con.Partners.update_one({"pk_id": partner_id}, {"$set": update})
    #         return {'statusCode': 200, 'body': 'Records updated and inserted successfully'}
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400, 'body': 'Updating or inserting components of stock in Partners failed'}

    # def cmsPartnerBBUpdateStockSaveComponents(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         partner_id = data['partner_id']
    #         update = data['update_stock']
    #         partner = db_con.Partners.find_one({"pk_id": partner_id})
    #         if not partner:
    #             partner = {"pk_id": partner_id, "all_attributes": {"available_stock": {"M-Kit": {}}}}
    #             db_con.Partners.insert_one(partner)
    #         available_stock = partner.get("all_attributes", {}).get("available_stock", {}).get("M-Kit", {})
    #         existing_mfr_part_numbers = {details.get("mfr_part_number"): key for key, details in
    #                                      available_stock.items()}
    #         existing_part_keys = set(available_stock.keys())
    #         combined_list = []
    #         next_key_index = 1
    #         for item in update:
    #             mfr_part_number = item.get("mfr_part_number")
    #             utilized_qty = str(item.get("utilized_qty", 0))
    #             damaged_qty = str(item.get("damaged_qty", 0))
    #             combined_qty = int(utilized_qty) + int(damaged_qty)
    #             current_qty = str(int(item.get("available_qty", 0)) - combined_qty)
    #             if mfr_part_number in existing_mfr_part_numbers:
    #                 part_key = existing_mfr_part_numbers[mfr_part_number]
    #                 existing_qty = int(available_stock[part_key].get("available_qty", 0))
    #                 new_qty = existing_qty - combined_qty
    #                 if new_qty < 0:
    #                     return {'statusCode': 400,
    #                             'body': f'utilized_qty and damaged_qty should not exceed available_qty'}
    #                 item["available_qty"] = str(new_qty)
    #                 item['bom_id'] = data['bom_id']
    #                 item['bom_name'] = data['bom_name']
    #                 combined_list.append((part_key, item))
    #             else:
    #                 if int(current_qty) < 0:
    #                     return {'statusCode': 400,
    #                             'body': f'utilized_qty and damaged_qty should not exceed available_qty'}
    #                 item["available_qty"] = current_qty
    #                 item['bom_id'] = data['bom_id']
    #                 item['bom_name'] = data.get('bom_name','-')
    #                 new_key = f"part{next_key_index}"
    #                 combined_list.append((new_key, item))
    #                 next_key_index += 1
    #         updates = []
    #         for part_key, item in combined_list:
    #             for k, v in item.items():
    #                 if k in ["utilized_qty", "damaged_qty"]:
    #                     current_value = int(available_stock.get(part_key, {}).get(k, 0))
    #                     updates.append({"pk_id": partner_id,
    #                                     f"all_attributes.available_stock.M-Kit.{data['bom_id']}.{part_key}.{k}": str(
    #                                         current_value + int(v))})
    #                 else:
    #                     updates.append({"pk_id": partner_id, f"all_attributes.available_stock.M-Kit.{data['bom_id']}.{part_key}.{k}": v})
    #         for update in updates:
    #             db_con.Partners.update_one({"pk_id": partner_id}, {"$set": update})
    #         return {'statusCode': 200, 'body': 'Records updated and inserted successfully'}
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400, 'body': 'Updating or inserting components of stock in Partners failed'}

    def cmsPartnerBBUpdateStockSaveComponents(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            partner_id = data['partner_id']
            updates = data['update_stock']
            partner = db_con.Partners.find_one_and_update(
                {"pk_id": partner_id},
                {"$setOnInsert": {"all_attributes.available_stock.M-Kit": {}}},
                upsert=True,
                return_document=True
            )
            available_stock = partner.get("all_attributes", {}).get("available_stock", {}).get("M-Kit", {}).get(
                data['bom_id'], {})
            existing_cmpt_ids = {details.get("cmpt_id"): key for key, details in available_stock.items()}
            combined_list = []
            next_key_index = 1
            for item in updates:
                cmpt_id = str(item.get("cmpt_id"))
                new_utilized_qty = int(item.get("utilized_qty", 0))
                new_damaged_qty = int(item.get("damaged_qty", 0))
                total_new_qty = new_utilized_qty + new_damaged_qty
                if cmpt_id in existing_cmpt_ids:
                    part_key = existing_cmpt_ids[cmpt_id]
                    existing_item = available_stock[part_key]
                    existing_utilized_qty = int(existing_item.get("utilized_qty", 0))
                    existing_damaged_qty = int(existing_item.get("damaged_qty", 0))
                    existing_available_qty = int(existing_item.get("available_qty", 0))
                    updated_utilized_qty = existing_utilized_qty + new_utilized_qty
                    updated_damaged_qty = existing_damaged_qty + new_damaged_qty
                    updated_available_qty = existing_available_qty - total_new_qty
                    if updated_available_qty < 0:
                        return {'statusCode': 400,
                                'body': 'utilized_qty and damaged_qty should not exceed available_qty'}
                    item.update({
                        "utilized_qty": updated_utilized_qty,
                        "damaged_qty": updated_damaged_qty,
                        "available_qty": updated_available_qty
                    })
                    combined_list.append((part_key, item))
                else:
                    initial_available_qty = int(item.get("available_qty", 0))
                    new_available_qty = initial_available_qty - total_new_qty
                    if new_available_qty < 0:
                        return {'statusCode': 400,
                                'body': 'utilized_qty and damaged_qty should not exceed available_qty'}
                    new_key = f"part{next_key_index}"
                    item["available_qty"] = new_available_qty
                    combined_list.append((new_key, item))
                    next_key_index += 1
            for part_key, item in combined_list:
                update_fields = {
                    f"all_attributes.available_stock.M-Kit.{data['bom_id']}.{part_key}.{k}": str(v) if isinstance(v,
                                                                                                                  int) else v
                    for k, v
                    in item.items()}
                db_con.Partners.update_one({"pk_id": partner_id}, {"$set": update_fields}, upsert=True)
            return {'statusCode': 200, 'body': 'Records updated and inserted successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Updating or inserting components of stock in Partners failed'}

    # def cmsPartnerBBUpdateStockFetch(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
    #         partner_id = data['partner_id']
    #         filter_bom_id = data['bom_id']
    #         filter = {'all_attributes.partner_id': partner_id, 'all_attributes.bom_id': filter_bom_id}
    #         bb = list(db_con.BoxBuilding.find(filter))
    #         partner = list(db_con.Partners.find({'all_attributes.partner_id': partner_id}))
    #         bb_cmpt, partner_cmpt = {}, {}
    #         for i in bb:
    #             all_attributes = i.get('all_attributes', {})
    #             for key, value in all_attributes.items():
    #                 if key.startswith('M_KIT'):
    #                     for k, v in value.items():
    #                         cmpt_id = v.get('cmpt_id')
    #                         provided_qty = v.get('provided_qty')
    #                         if cmpt_id and provided_qty is not None:
    #                             bb_cmpt[cmpt_id] = bb_cmpt.get(cmpt_id, 0) + int(provided_qty)
    #         for i in partner:
    #             all_attributes = i.get('all_attributes', {})
    #             available_stock = all_attributes.get('available_stock', {})
    #             for kit, parts in available_stock.items():
    #                 for part, details in parts.items():
    #                     cmpt_id = details.get('cmpt_id')
    #                     utilized_qty = int(details.get('utilized_qty', 0))
    #                     damaged_qty = int(details.get('damaged_qty', 0))
    #                     if cmpt_id:
    #                         partner_cmpt[cmpt_id] = utilized_qty + damaged_qty
    #         mkit_data = list(db_con.BoxBuilding.find({'all_attributes.partner_id': partner_id}))
    #         bom_ids = [item['all_attributes']['bom_id'] for item in mkit_data]
    #         bom_constraints = list(db_con.BOM.find({'pk_id': {'$in': bom_ids}}))
    #         bom_id_list = [item['pk_id'] for item in bom_constraints]
    #         parts, all_mfr_prt_nums = [], set()
    #         bom_ids_to_process = bom_id_list if filter_bom_id == 'All' else [filter_bom_id]
    #         for bom_id in bom_ids_to_process:
    #             query = list(db_con.BoxBuilding.find({'all_attributes.bom_id': bom_id}))
    #             res = [{key: value for key, value in i.get('all_attributes', {}).items() if key.startswith('M_KIT')} for
    #                    i in query]
    #             all_mfr_prt_nums.update(
    #                 [part_value['vic_part_number'] for entry in res for kit, parts in entry.items() for part, part_value
    #                  in parts.items()])
    #         inventory_items = list(
    #             db_con.Inventory.find({'mfr_prt_num': {'$in': list(all_mfr_prt_nums)}}, {'mfr_prt_num': 1, 'qty': 1}))
    #         inventory_qty_map = {item['mfr_prt_num']: item.get('qty', 0) for item in inventory_items}
    #         part_counter = 1
    #         for bom_id in bom_ids_to_process:
    #             query = list(db_con.BoxBuilding.find({'all_attributes.bom_id': bom_id}))
    #             res = [{key: value for key, value in i.get('all_attributes', {}).items() if key.startswith('M_KIT')} for
    #                    i in query]
    #             parts_list = [value for dictionary in res for key, value in dictionary.items()]
    #             for part in parts_list:
    #                 for part_key, part_value in part.items():
    #                     part_value['utilized_qty'] = '0'
    #                     part_value['damaged_qty'] = part_value.get('damaged_qty', '0')
    #                     cmpt_id = part_value['cmpt_id']
    #                     part_value['available_qty'] = f"{bb_cmpt.get(cmpt_id, 0) - partner_cmpt.get(cmpt_id, 0)}"
    #                     part_value['bom_id'] = bom_id if filter_bom_id == 'All' else None
    #                     parts.append({f"part{part_counter}": part_value})
    #                     part_counter += 1
    #         final_parts_dict = {}
    #         for item in parts:
    #             for part_key, part_value in item.items():
    #                 mfr_part_number = part_value['vic_part_number']
    #                 if mfr_part_number not in final_parts_dict:
    #                     final_parts_dict[mfr_part_number] = part_value
    #                 else:
    #                     current = final_parts_dict[mfr_part_number]
    #                     # current['provided_qty'] = max(current.get('provided_qty', 0), part_value.get('provided_qty', 0))
    #                     current_provided_qty = current.get('provided_qty', 0)
    #                     part_value_provided_qty = part_value.get('provided_qty', 0)
    #                     if isinstance(current_provided_qty, str):
    #                         current_provided_qty = int(current_provided_qty)
    #                     if isinstance(part_value_provided_qty, str):
    #                         part_value_provided_qty = int(part_value_provided_qty)
    #                     current['provided_qty'] = max(current_provided_qty, part_value_provided_qty)
    #                     current['utilized_qty'] = max(current.get('utilized_qty', 0), part_value.get('utilized_qty', 0))
    #                     current['damaged_qty'] = max(current.get('damaged_qty', 0), part_value.get('damaged_qty', 0))
    #                     current['available_qty'] = max(current.get('available_qty', 0),
    #                                                    part_value.get('available_qty', 0))
    #                     final_parts_dict[mfr_part_number] = current
    #         final_parts = [{key: value for key, value in part_value.items() if key != 'sno'} for part_value in
    #                        final_parts_dict.values()]
    #         return {'statusCode': 200, 'body': final_parts}
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         return {'statusCode': 400, 'body': f'Unable to fetch data: {err}, {f_name}, {line_no}'}

    def cmsPartnerBBUpdateStockFetch(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            partner_id = data['partner_id']
            filter_bom_id = data['bom_id']
            filter = {'all_attributes.partner_id': partner_id, 'all_attributes.bom_id': filter_bom_id}
            bb = list(db_con.BoxBuilding.find(filter))
            partner = list(db_con.Partners.find({'all_attributes.partner_id': partner_id},{'_id':0,'all_attributes':1}))
            if not partner:
                return {'statusCode': 400, 'body': 'Partner not found'}
            available_stock = partner[0].get('all_attributes', {}).get('available_stock', {})
            bb_cmpt = {}
            partner_cmpt = {}
            for bom_id, parts in available_stock.get("M-Kit", {}).items():
                if filter_bom_id in available_stock.get("M-Kit", {}):
                    if filter_bom_id == 'All' or bom_id == filter_bom_id:
                        for part_key, part_data in parts.items():
                            cmpt_id = part_data.get('cmpt_id')
                            provided_qty = part_data.get('provided_qty')
                            utilized_qty = int(part_data.get('utilized_qty', 0))
                            damaged_qty = int(part_data.get('damaged_qty', 0))
                            if cmpt_id and provided_qty is not None:
                                if cmpt_id in bb_cmpt:
                                    bb_cmpt[cmpt_id] += int(provided_qty)
                                else:
                                    bb_cmpt[cmpt_id] = int(provided_qty)
                            if cmpt_id:
                                partner_cmpt[cmpt_id] = utilized_qty + damaged_qty
                        mkit_data = list(db_con.BoxBuilding.find({'all_attributes.partner_id': partner_id}))
                        bom_ids = [item['all_attributes']['bom_id'] for item in mkit_data]
                        bom_constraints = list(db_con.BOM.find({'pk_id': {'$in': bom_ids}}))
                        bom_id_list = [item['pk_id'] for item in bom_constraints]
                        parts = []
                        all_mfr_prt_nums = set()
                        bom_ids_to_process = bom_id_list if filter_bom_id == 'All' else [filter_bom_id]
                        for bom_id in bom_ids_to_process:
                            query = list(db_con.BoxBuilding.find({'all_attributes.bom_id': bom_id}))
                            res = [{key: value for key, value in i.get('all_attributes', {}).items() if
                                    key.startswith('M_KIT')} for
                                   i in query]
                            all_mfr_prt_nums.update(
                                [part_value.get('vic_part_number') for entry in res for kit, parts in entry.items() for
                                 part, part_value in parts.items()]
                            )
                        inventory_items = list(
                            db_con.Inventory.find({'mfr_prt_num': {'$in': list(all_mfr_prt_nums)}}, {'mfr_prt_num': 1, 'qty': 1}))
                        inventory_qty_map = {item.get('mfr_prt_num'): item.get('qty', 0) for item in inventory_items}
                        part_counter = 1
                        for bom_id in bom_ids_to_process:
                            query = list(db_con.BoxBuilding.find({'all_attributes.bom_id': bom_id}))
                            res = [{key: value for key, value in i.get('all_attributes', {}).items() if
                                    key.startswith('M_KIT')} for
                                   i in query]
                            parts_list = [value for dictionary in res for key, value in dictionary.items()]
                            for part in parts_list:
                                for part_key, part_value in part.items():
                                    cmpt_id = part_value.get('cmpt_id')
                                    utilized_qty = int(part_value.get('utilized_qty', 0))
                                    damaged_qty = int(part_value.get('damaged_qty', 0))
                                    provided_qty = int(part_value.get('provided_qty', 0))
                                    available_qty = bb_cmpt.get(cmpt_id, 0) - partner_cmpt.get(cmpt_id, 0)
                                    part_value['available_qty'] = max(0, available_qty)
                                    part_value['bom_id'] = bom_id if filter_bom_id == 'All' else None
                                    part_value['utilized_qty'] = utilized_qty
                                    part_value['damaged_qty'] = damaged_qty
                                    part_value['provided_qty'] = provided_qty
                                    parts.append({f"part{part_counter}": part_value})
                                    part_counter += 1
                        final_parts_dict = {}
                        for item in parts:
                            for part_key, part_value in item.items():
                                mfr_part_number = part_value.get('vic_part_number')
                                if mfr_part_number not in final_parts_dict:
                                    final_parts_dict[mfr_part_number] = part_value
                                else:
                                    current = final_parts_dict[mfr_part_number]
                                    current['provided_qty'] = max(current.get('provided_qty', 0),
                                                                  part_value.get('provided_qty', 0))
                                    current['utilized_qty'] = max(current.get('utilized_qty', 0),
                                                                  part_value.get('utilized_qty', 0))
                                    current['damaged_qty'] = max(current.get('damaged_qty', 0),
                                                                 part_value.get('damaged_qty', 0))
                                    current['available_qty'] = max(current.get('available_qty', 0),
                                                                   part_value.get('available_qty', 0))
                                    final_parts_dict[mfr_part_number] = current
                        final_parts = [{key: value for key, value in part_value.items() if key != 'sno'} for part_value
                                       in
                                       final_parts_dict.values()]
                        final_parts_updated = [{key: value for key, value in part.items() if key != 'sno'} for part in
                                               final_parts]
                        final_parts_updated = [{**i, 'bom_id': data['bom_id']} if 'bom_id' in i else i for i in
                                               final_parts_updated]
                        return {'statusCode': 200, 'body': final_parts_updated}
            for i in bb:
                all_attributes = i.get('all_attributes', {})
                for key, value in all_attributes.items():
                    if key.startswith('M_KIT'):
                        for k, v in value.items():
                            cmpt_id = v.get('cmpt_id')
                            provided_qty = v.get('provided_qty')
                            if cmpt_id and provided_qty is not None:
                                if cmpt_id in bb_cmpt:
                                    bb_cmpt[cmpt_id] += int(provided_qty)
                                else:
                                    bb_cmpt[cmpt_id] = int(provided_qty)
            all_attributes = partner[0].get('all_attributes', {})
            available_stock = all_attributes.get('available_stock', {})
            for kit, parts in available_stock.items():
                for part, details in parts.items():
                    cmpt_id = details.get('cmpt_id')
                    utilized_qty = int(details.get('utilized_qty', 0))
                    damaged_qty = int(details.get('damaged_qty', 0))
                    if cmpt_id:
                        partner_cmpt[cmpt_id] = utilized_qty + damaged_qty
            mkit_data = list(db_con.BoxBuilding.find({'all_attributes.partner_id': partner_id}))
            bom_ids = [item['all_attributes']['bom_id'] for item in mkit_data]
            bom_constraints = list(db_con.BOM.find({'pk_id': {'$in': bom_ids}}))
            bom_id_list = [item['pk_id'] for item in bom_constraints]
            parts = []
            all_mfr_prt_nums = set()
            bom_ids_to_process = bom_id_list if filter_bom_id == 'All' else [filter_bom_id]
            for bom_id in bom_ids_to_process:
                query = list(db_con.BoxBuilding.find({'all_attributes.bom_id': bom_id}))
                res = [{key: value for key, value in i.get('all_attributes', {}).items() if
                        key.startswith('M_KIT')} for
                       i in query]
                all_mfr_prt_nums.update(
                    [part_value.get('vic_part_number') for entry in res for kit, parts in entry.items() for
                     part, part_value
                     in parts.items()]
                )
            inventory_items = list(db_con.Inventory.find({'mfr_prt_num': {'$in': list(all_mfr_prt_nums)}},
                                                         {'mfr_prt_num': 1, 'qty': 1}))
            inventory_qty_map = {item.get('mfr_prt_num'): item.get('qty', 0) for item in inventory_items}
            part_counter = 1
            for bom_id in bom_ids_to_process:
                query = list(db_con.BoxBuilding.find({'all_attributes.bom_id': bom_id}))
                res = [{key: value for key, value in i.get('all_attributes', {}).items() if
                        key.startswith('M_KIT')} for
                       i in query]
                parts_list = [value for dictionary in res for key, value in dictionary.items()]
                for part in parts_list:
                    for part_key, part_value in part.items():
                        part_value['utilized_qty'] = '0'
                        part_value['damaged_qty'] = part_value.get('damaged_qty', '0')
                        cmpt_id = part_value.get('cmpt_id')
                        part_value[
                            'available_qty'] = f"{bb_cmpt.get(cmpt_id, 0) - partner_cmpt.get(cmpt_id, 0)}"
                        part_value['bom_id'] = bom_id if filter_bom_id == 'All' else None
                        parts.append({f"part{part_counter}": part_value})
                        part_counter += 1
            final_parts_dict = {}
            for item in parts:
                for part_key, part_value in item.items():
                    mfr_part_number = part_value.get('vic_part_number')
                    if mfr_part_number not in final_parts_dict:
                        final_parts_dict[mfr_part_number] = part_value
                    else:
                        current = final_parts_dict[mfr_part_number]
                        current['provided_qty'] = max(current.get('provided_qty', 0),
                                                      part_value.get('provided_qty', 0))
                        current['utilized_qty'] = max(current.get('utilized_qty', 0),
                                                      part_value.get('utilized_qty', 0))
                        current['damaged_qty'] = max(current.get('damaged_qty', 0),
                                                     part_value.get('damaged_qty', 0))
                        current['available_qty'] = max(current.get('available_qty', 0),
                                                       part_value.get('available_qty', 0))
                        final_parts_dict[mfr_part_number] = current
            final_parts = [{key: value for key, value in part_value.items() if key != 'sno'} for part_value in
                           final_parts_dict.values()]
            final_parts = [
                part for part in final_parts
                if 'ptg_prt_num' in part
            ]
            final_parts_updated = [
                {key: value for key, value in part.items() if key != 'sno'}
                for part in final_parts
            ]
            final_parts_updated = [{**i, 'bom_id': data['bom_id']} if 'bom_id' in i else i for i in
                                   final_parts_updated]
            return {'statusCode': 200, 'body': final_parts_updated}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400, 'body': f'Unable to fetch data: {err}, {f_name}, {line_no}'}

    def cmsPartnerGetStockBoxbuilding(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            partner_id = data['partner_id']
            query = list(db_con.Partners.find({'all_attributes.partner_id': partner_id}, {'_id': 0,'all_attributes.available_stock':1}))
            stock_details_list = [item['all_attributes']['available_stock'].get('M-Kit',()) for item in query]
            # if stock_details_list:
            return {"statusCode":200,"body":stock_details_list}
            # else:
            #     return {"statusCode": 404, "body": "NO DATA FOUND"}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
           #return {'statusCode': 400,'error': 'There is an AWS Lambda Data Capturing Error'}uu
            return {'statusCode': 400, 'body': 'unable to fetch box building data'} 
        
    def CmsGetPartnerDetails(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            partners = list(db_con.Partners.find({"lsi_key": "Active"}))
            lst = sorted([{
                'partner_id': item.get('pk_id', ""),
                'partner_name': item.get('all_attributes', {}).get('partner_name', ""),
                'partner_type': [
                    "".join([value for value in item.get('all_attributes', {}).get('partner_type', [])])],
                'contact_details': item.get('all_attributes', {}).get('contact_number', ""),
                'email': item.get('all_attributes', {}).get('email', "")
            } for item in partners], key=lambda x: x['partner_id'], reverse=False)
            conct.close_connection(client)
            return {'statusCode': 200, 'body': lst}
        except Exception as err:
            exc_type, _, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request (check data)'}
        
    def cmsGetPartnerDC(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            res = list(db_con.NewPurchaseOrder.find({'all_attributes.partner_id': data['partner_id'], 'gsipk_table': 'ServiceOrder', 'lsi_key': 'Approved'}))
            so_ids = [i['all_attributes']['so_id'] for i in res]
            delivery_challan_records = db_con.DeliveryChallan.find({
                "all_attributes.so_id": {"$in": so_ids}
            })
            so_id_qty_totals = {so_id: 0 for so_id in so_ids}
            for dc_record in delivery_challan_records:
                so_id = dc_record["all_attributes"]["so_id"]
                if "M_parts" in dc_record["all_attributes"]:
                    for part in dc_record["all_attributes"]["M_parts"].values():
                        so_id_qty_totals[so_id] += int(part.get("provided_qty", 0))
                if "E_parts" in dc_record["all_attributes"]:
                    for part in dc_record["all_attributes"]["E_parts"].values():
                        so_id_qty_totals[so_id] += int(part.get("provided_qty", 0))
            result = []
            for i in res:
                date_str = i['all_attributes']['primary_document_details']['so_date']
                start_date = datetime.strptime(date_str, "%Y-%m-%d")
                current_date = datetime.now()
                days_difference = (start_date - current_date).days
                if days_difference < 0:
                    days_difference = 0
                document_no = i['all_attributes']['so_id']
                qty_from_delivery_challan = so_id_qty_totals.get(document_no, 0)
                response = {
                    'order_no': i['pk_id'],
                    'company_name': i['all_attributes']['ship_to']['company_name'],
                    'transaction_name': i['all_attributes']['primary_document_details'].get('transaction_name', '-'),
                    "document_no": i['all_attributes']['so_id'],
                    'qty': str(qty_from_delivery_challan),
                    'due_date': str(days_difference)+" Days Left",
                    'stock': 'In Stock',
                    'status': 'In Progress'
                }
                result.append(response)
            return {'statusCode': 200, 'body': result}
        except Exception as err:
            exc_type, _, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request (check data)'}
        
    def cmsPartnerInStock(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            client = db_conct['client']
            res = list(db_con.DeliveryChallan.find({'all_attributes.so_id': data['so_id']}))
            dc_ids = []
            combined_parts = {}
            for i in res:
                dc_ids.append(i['all_attributes']['dc_id'])
                for key in ["M_parts", "E_parts"]:
                    if key in i['all_attributes']:
                        for part_key, part_value in i['all_attributes'][key].items():
                            mfr_prt_num = part_value.get("mfr_prt_num")
                            if mfr_prt_num in combined_parts:
                                combined_parts[mfr_prt_num]["provided_qty"] += int(part_value.get("provided_qty", 0))
                            else:
                                combined_parts[mfr_prt_num] = {
                                    'part_name': part_value.get('prdt_name') or part_value.get('sub_ctgr'),
                                    'mfr_prt_num': mfr_prt_num,
                                    'ctgr_name': part_value.get('ctgr_name'),
                                    'department': part_value.get('department'),
                                    'provided_qty': int(part_value.get("provided_qty", 0))
                                }
            response = []
            result = db_con.ServiceOrderGateEntry.find({'gsisk_id': {'$in': dc_ids}})
            received_qty_sum = {}
            if result:
                for i in result:
                    parts = i.get('all_attributes', {}).get('parts', {})
                    for part in parts.values():
                        mfr_prt_num = part.get('mfr_prt_num')
                        received_qty = int(part.get('received_qty', 0))
                        if mfr_prt_num:
                            received_qty_sum[mfr_prt_num] = received_qty_sum.get(mfr_prt_num, 0) + received_qty
                for part in combined_parts.values():
                    rec_qty = str(received_qty_sum.get(part['mfr_prt_num'], 0))
                    available_qty = str(int(part['provided_qty']) - int(rec_qty))
                    response.append({
                        'part_name': part['part_name'],
                        'mfr_prt_num': part['mfr_prt_num'],
                        'ctgr_name': part['ctgr_name'],
                        'department': part['department'],
                        'req_qty': str(part['provided_qty']),
                        # 'rec_qty': str(received_qty_sum.get(part['mfr_prt_num'])),
                        'rec_qty': rec_qty,
                        'available_qty': available_qty
                    })
                for i in response:
                    if int(i['available_qty']) < 0:
                        i["status"] = "Out of Stock"
                    elif 0 <= int(i['available_qty']) <= 100:
                        i["status"] = "Running out of Stock"
                    else:
                        i["status"] = "In Stock"
                return {'statusCode': 200, 'body': response}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
            return {'statusCode': 400, 'body': 'Bad Request(check data)'}