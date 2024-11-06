import json
from datetime import datetime, timedelta
from db_connection import db_connection_manage
import re
import sys

conct = db_connection_manage()

class Users:     
    #Save User
    def createUser(request_body):
        try:
                data = request_body
                env_type = data['env_type']
                db_conct = conct.get_conn(env_type)
                db_con = db_conct['db']
                sk_timeStamp = (datetime.now()).isoformat()
                
                pk_ids = list(db_con.Users.find({'pk_id': {'$regex': '^PTGUSER'}}, {'pk_id': 1}))
                if len(pk_ids) == 0: 
                    max_pk = 1
                else:
                    pk_filter = [int(x['pk_id'][8:]) for x in pk_ids]
                    max_pk = max(pk_filter) + 1

                # Create the new user_id
                new_user_id = f'PTGUSER_{max_pk}'

                # Extract the details from the request body
                users_data = {
                    'user_id': new_user_id,
                    'first_name': data.get('first_name').strip(),
                    'last_name': data.get('last_name').strip(),
                    'phone_no': data.get('phone_no').strip(),
                    'email': data.get('email').strip(),  # Normalize email to lowercase and remove whitespace
                    'role_id': data.get('role_id').strip(),
                    'token': ''
                }

                # Check if the PhoneNo or Email already exists in the list collection
                existing_record = db_con.Users.find_one({
                    "$or": [
                        {'all_attributes.phone_no': users_data['phone_no']},
                        {'all_attributes.email': users_data['email']}
                    ]
                })
                if existing_record:
                        if existing_record.get("all_attributes", {}).get("phone_no") == users_data['phone_no']:
                            return {'statusCode': 400, 'body': 'PhoneNo already exists'}
                        if existing_record.get("all_attributes", {}).get("email") == users_data['email']:
                            return {'statusCode': 400, 'body': 'Email already exists'}

                # Construct the document to be inserted
                userData = {
                    'pk_id': users_data['user_id'],
                    'all_attributes': users_data,
                    'created_on': sk_timeStamp,
                    'gsipk_table': 'Users',
                    'gsipk_id': '',
                    'lsi_key': '',
                }

                # Insert the userData into the Users collection
                db_con.Users.insert_one(userData)

                return {'statusCode': 200, 'body': 'Data inserted successfully'}
    
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400, 'body': 'User Creation Failed'}

    #get All list of Users      
    # def getUsers(request_body):
    #     try:
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
            
    #         get_users = list(db_con.Users.find({}, {"_id": 0}))
    #         return {'statusCode': 200, 'body': get_users}
            
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         #print(f"Error {exc_type.__name__} in file {f_name}, line {line_no}: {err}")
    #         return {'statusCode': 400,'body': 'No Records Found'}   

    def getUsers(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            # Fetch users from the Users table
            users = list(db_con.Users.find({}, {"_id": 0}))
            
            # Fetch all roles in advance and store them in a dictionary for fast lookup
            roles = list(db_con.UserRoles.find({}, {"_id": 0, "all_attributes.role_id": 1, "all_attributes.role_name": 1}))
            role_map = {role['all_attributes']['role_id']: role['all_attributes']['role_name'] for role in roles}
            
            # For each user, fetch the role_name from the pre-built role_map dictionary
            for user in users:
                role_id = user['all_attributes']['role_id']
                
                # Look up role_name in the role_map
                user['all_attributes']['role_name'] = role_map.get(role_id, None)  # Default to None if role_id not found
                
            return {'statusCode': 200, 'body': users}
        
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            # Log error and return 400 status
            return {'statusCode': 400, 'body': 'No Records Found'}

 
        
    #delete user by user_id
    def deleteUser(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            # Extract user_id from the request body
            user_id = data.get('user_id')
            
            if not user_id:
                return {'statusCode': 400, 'body': 'user_id is required'}

            # Check if the user exists
            existing_record = db_con.Users.find_one({'all_attributes.user_id': user_id})
            if not existing_record:
                return {'statusCode': 404, 'body': 'User not found'}
            
            # Delete the user by user_id
            result = db_con.Users.delete_one({'all_attributes.user_id': user_id})

            if result.deleted_count == 0:
                    return {'statusCode': 500, 'body': 'User Deletion Failed'}

            return {'statusCode': 200, 'body': 'User deleted successfully'}
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno           
            return {'statusCode': 400, 'body': 'User Deletion Failed'}
        

        
    #Update User        

    def updateUser(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            # Extract the user_id and fields to be updated from the request body
            user_id = data.get('user_id')
            if not user_id:
                return {'statusCode': 400, 'body': 'User ID is required for update'}

            # Prepare the update fields
            update_fields = {}
            if 'first_name' in data:
                update_fields['all_attributes.first_name'] = data['first_name'].strip()
            if 'last_name' in data:
                update_fields['all_attributes.last_name'] = data['last_name'].strip()
            if 'phone_no' in data:
                update_fields['all_attributes.phone_no'] = data['phone_no'].strip()
            if 'role_id' in data:
                update_fields['all_attributes.role_id'] = data['role_id'].strip()    
            if 'email' in data:
                update_fields['all_attributes.email'] = data['email'].strip() # Normalize email to lowercase

            if not update_fields:
                return {'statusCode': 400, 'body': 'No fields provided for update'}

            # Check if the new PhoneNo or Email already exists for a different user
            if 'phone_no' in update_fields or 'email' in update_fields or 'role_id' in update_fields :
                existing_record = db_con.Users.find_one({
                    "$and": [
                        {"all_attributes.user_id": {"$ne": user_id}},
                        {
                            "$or": [
                                {'all_attributes.phone_no': update_fields.get('all_attributes.phone_no')},
                                {'all_attributes.email': update_fields.get('all_attributes.email')},
                                {'all_attributes.role_id': update_fields.get('all_attributes.role_id')}
                            ]
                        }
                    ]
                })

                if existing_record:
                    if existing_record.get("all_attributes", {}).get("phone_no") == update_fields.get('all_attributes.phone_no'):
                        return {'statusCode': 400, 'body': 'PhoneNo already exists for another user'}
                    if existing_record.get("all_attributes", {}).get("email") == update_fields.get('all_attributes.email'):
                        return {'statusCode': 400, 'body': 'Email already exists for another user'}

            # Update the user record in the Users collection
            result = db_con.Users.update_one(
                {'all_attributes.user_id': user_id}, 
                {'$set': update_fields}
            )

            if result.matched_count == 0:
                return {'statusCode': 404, 'body': 'User not found'}

            return {'statusCode': 200, 'body': 'User updated successfully'}
        
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400, 'body': 'User Update Failed: {exc_type.__name__} in {f_name} at line {line_no}: {err}'}

    #userdetails by ID


    def getUserById(request_body):
        try:
            # Extract the user_id from the request body
            data = request_body
            user_id = data.get('user_id').strip()
            
            # Get the database connection
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            # Fetch the user document by user_id with a specific projection (only fetch required fields)
            user_record = db_con.Users.find_one(
                {'all_attributes.user_id': user_id}, 
                {'_id': 0, 'pk_id': 1, 'all_attributes': 1, 'created_on': 1}
            )
            
            if user_record:
                # Return only the specified fields in the response
                return {'statusCode': 200, 'body': user_record}
            else:
                # Return a 404 response if user not found
                return {'statusCode': 404, 'body': f'User with user_id {user_id} not found'}
        
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400, 'body': 'User with user_id not found Error: {exc_type.__name__} in {f_name} at line {line_no}: {err}'}   


            # Update token api

    def updateUserToken(request_body):
        try:
            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            email = data.get('email').strip().lower()  # Normalize email to lowercase
            token = data.get('token').strip()  # Token passed in the request
            last_login_time = (datetime.now()).isoformat()  # Current login time

            # Find the user by email
            existing_user = db_con.Users.find_one({'all_attributes.email': email})

            if not existing_user:
                return {'statusCode': 400, 'body': 'User with this email does not exist'}

            # Update the token and login_time for the user
            db_con.Users.update_one(
                {'all_attributes.email': email},  # Find user by email
                {'$set': {
                    'all_attributes.token': token,  # Update token field
                    'all_attributes.last_login_time': last_login_time  # Add login time field
                }}
            )

            return {'statusCode': 200, 'body': 'Token and login time updated successfully'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400, 'body': 'Failed to update token: {exc_type.__name__} in {f_name} at line {line_no}: {err}'}
        # {
        # "env_type": "your_environment",
        # "email": "user@example.com",
        # "token": "new_token_value"
        # }

    #  userlogs

    # def saveUserLoginLog(request_body):
    #     try:
    #         # Extract request data
    #         data = request_body
    #         env_type = data['env_type']
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
            
    #         # Extract email and normalize it to lowercase
    #         email = data.get('email').strip().lower()
    #         login_time = datetime.now().isoformat()

    #         # Find the user by email
    #         user = db_con.Users.find_one({'all_attributes.email': email})

    #         if not user:
    #             return {'statusCode': 400, 'body': 'User with this email does not exist'}

    #         # Extract required details from the user document
    #         user_id = user['all_attributes'].get('user_id')
    #         first_name = user['all_attributes'].get('first_name')
    #         last_name = user['all_attributes'].get('last_name')
    #         role_id = user['all_attributes'].get('role_id')
    #         phone_no = user['all_attributes'].get('phone_no')

    #         # Construct the log entry for the UserLogs collection
    #         user_log_data = {
    #             'user_id': user_id,
    #             'email': email,
    #             'username': f"{first_name} {last_name}",  # Combine first and last name for username
    #             'role_id': role_id,
    #             'phone_no': phone_no,
    #             'login_time': login_time
    #         }

    #         # Insert log into the UserLogs collection
    #         db_con.UserLogs.insert_one(user_log_data)

    #         return {'statusCode': 200, 'body': 'User login details saved successfully'}

    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         return {'statusCode': 400, 'body': f'Failed to save user login log: {exc_type.__name__} in {f_name} at line {line_no}: {err}'}   
            
    #     {
    #     "env_type": "development",
    #     "email": "user@example.com"
          # }



    # def saveUserLoginLog(request_body):
    #     try:
    #         # Ensure request body is not None
    #         if not request_body:
    #             return {'statusCode': 400, 'body': 'Request body is empty'}
    #         # Extract request data
    #         data = request_body
    #         env_type = data.get('env_type')
    #         email = data.get('email')
    #         # Validate env_type and email
    #         if not env_type:
    #             return {'statusCode': 400, 'body': 'env_type is missing in the request'}
    #         if not email:
    #             return {'statusCode': 400, 'body': 'Email is missing in the request'}

    #         # Normalize email and remove extra spaces
    #         email = email.strip().lower()
    #         login_time = datetime.now().isoformat()

    #         # Get the database connection based on env_type
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']

    #         # Print all users for debugging
    #         users = list(db_con.Users.find({}))
    #         print(f"All users in the database: {users}")

    #         # Find the user by email (case-insensitive)
    #         user = db_con.Users.find_one({'all_attributes.email': {'$regex': f'^{email}$', '$options': 'i'}})
    #         print(f"User found: {user}")

    #         if not user:
    #             return {'statusCode': 400, 'body': 'User with this email does not exist'}

    #         # Extract required details from the user document
    #         user_id = user['all_attributes'].get('user_id')
    #         first_name = user['all_attributes'].get('first_name')
    #         last_name = user['all_attributes'].get('last_name')
    #         role_id = user['all_attributes'].get('role_id')
    #         phone_no = user['all_attributes'].get('phone_no')

    #         # Construct the log entry for the UserLogs collection
    #         user_log_data = {
    #             'user_id': user_id,
    #             'email': email,
    #             'username': f"{first_name} {last_name}",  # Combine first and last name for username
    #             'role_id': role_id,
    #             'phone_no': phone_no,
    #             'login_time': login_time
    #         }

    #         # Log the user log data for debugging
    #         print(f"User log data to be saved: {user_log_data}")

    #         # Insert log into the UserLogs collection
    #         db_con.UserLogs.insert_one(user_log_data)

    #         return {'statusCode': 200, 'body': 'User login details saved successfully'}

    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         return {'statusCode': 400, 'body': f'Failed to save user login log: {exc_type.__name__} in {f_name} at line {line_no}: {err}'}

    
    def saveUserLoginLog(request_body):
        try:            

            data = request_body
            env_type = data['env_type']
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            email = data.get('email')
            # Normalize email and remove extra spaces
            email = email.strip().lower()
            login_time = datetime.now().isoformat()           

            # Check if the collection exists
            # print(f"Collections in database: {db_con.list_collection_names()}")
            # Check if Users collection has data
            users = list(db_con.Users.find({}))
            # print("All users in the Users collection",users)
            # Find the user by email (case-insensitive)
            user = db_con.Users.find_one({'all_attributes.email': {'$regex': f'^{email}$', '$options': 'i'}})
            # print(f"User found: {user}")
            if not user:
                return {'statusCode': 400, 'body': 'User with this email does not exist'}

            # Extract required details from the user document
            user_id = user['all_attributes'].get('user_id')
            first_name = user['all_attributes'].get('first_name')
            last_name = user['all_attributes'].get('last_name')
            role_id = user['all_attributes'].get('role_id')
            phone_no = user['all_attributes'].get('phone_no')

            # Construct the log entry for the UserLogs collection
            user_log_data = {               
                'user_id': user_id,
                'email': email,
                'username': f"{first_name} {last_name}",  # Combine first and last name for username
                'role_id': role_id,
                'phone_no': phone_no,
                'login_time': login_time
            }

            # Log the user log data for debugging
            # print(f"User log data to be saved: {user_log_data}")

            # Insert log into the UserLogs collection
            db_con.UserLogs.insert_one(user_log_data)

            return {'statusCode': 200, 'body': 'User login details saved successfully'}

        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400, 'body': 'Failed to save user login log'}
        

    # def getUserScreenpermissionsDetails(request_body):
        
    #     try:
    #         # Extract data from the request
    #         data = request_body
    #         email = data.get('email')
    #         env_type = data['env_type']            
    #         # Establish the database connection
    #         db_conct = conct.get_conn(env_type)
    #         db_con = db_conct['db']
            
    #         # Fetch the user details from the Users collection based on the email
    #         user = db_con.Users.find_one({"all_attributes.email": email}, {"_id": 0})
            
    #         if not user:
    #             return {'statusCode': 404, 'body': f'User with email {email} not found'}
            
    #         # Extract role_id from the user's details
    #         role_id = user['all_attributes'].get('role_id')
            
    #         if not role_id:
    #             return {'statusCode': 400, 'body': 'Role ID not found for the user'}
            
    #         # Fetch the role details from the UserRoles collection
    #         role = db_con.UserRoles.find_one({"all_attributes.role_id": role_id}, {"_id": 0, "all_attributes.role_name": 1})
    #         print(role)
    #         if not role:
    #             return {'statusCode': 404, 'body': f'Role with role_id {role_id} not found'}
            
    #         role_name = role['all_attributes'].get('role_name')
    #         print(role_name,'rolename om')
            
    #         # Fetch the screen permissions from the RolePermissions collection
    #         # role_permissions = db_con.RolePermissions.find_one({"role_name": role_name}, {"_id": 0, "screen_permissions": 1})
    #         role_permissions = db_con.RolePermissions.find_one({"all_attributes.role_name": role_name}, {"_id": 0, "all_attributes.screen_permissions": 1})
    #         print(role_permissions,'ommm pm')
            
    #         if not role_permissions:
    #             return {'statusCode': 404, 'body': f'Screen permissions not found for role {role_name}'}
            
    #         # Prepare the final response
    #         user_details = {
    #             "email": user['all_attributes']['email'],
    #             "first_name": user['all_attributes']['first_name'],
    #             "last_name": user['all_attributes']['last_name'],
    #             "role_name": role_name,
    #             "screen_permissions": role_permissions.get('screen_permissions', {})
    #         }
        
    #         return {'statusCode': 200, 'body': user_details}
    #     except Exception as err:
    #         exc_type, exc_obj, tb = sys.exc_info()
    #         f_name = tb.tb_frame.f_code.co_filename
    #         line_no = tb.tb_lineno
    #         return {'statusCode': 400, 'body': 'User with this email permissions does not exist'}
    
    # {
    # "env_type": "Development",
    # "email": "om663@gmail.com"
    # }

    def getUserScreenpermissionsDetails(request_body):
        try:
            # Extract data from the request
            data = request_body
            email = data.get('email')
            env_type = data['env_type']            
            # Establish the database connection
            db_conct = conct.get_conn(env_type)
            db_con = db_conct['db']
            
            # Fetch the user details from the Users collection based on the email
            # user = db_con.Users.find_one({"all_attributes.email": email}, {"_id": 0})
            user = db_con.Users.find_one({"all_attributes.email": {"$regex": f"^{email}$", "$options": "i"}}, {"_id": 0})
            
            if not user:
                return {'statusCode': 404, 'body': f'User with email {email} not found'}
            
            # Extract role_id from the user's details
            role_id = user['all_attributes'].get('role_id')
            
            if not role_id:
                return {'statusCode': 400, 'body': 'Role ID not found for the user'}
            
            # Fetch the role details from the UserRoles collection
            role = db_con.UserRoles.find_one({"all_attributes.role_id": role_id}, {"_id": 0, "all_attributes.role_name": 1, "all_attributes.role_id": 1})
            
            if not role:
                return {'statusCode': 404, 'body': f'Role with role_id {role_id} not found'}
            
            role_name = role['all_attributes'].get('role_name')
            role_id = role['all_attributes'].get('role_id')
            
            # Fetch the screen permissions from the RolePermissions collection
            role_permissions = db_con.RolePermissions.find_one({"all_attributes.role_name": role_name}, {"_id": 0, "all_attributes.screen_permissions": 1})
            
            if not role_permissions:
                return {'statusCode': 404, 'body': f'Screen permissions not found for  {user['all_attributes']['email']}'}
            
            # Access the screen_permissions within the all_attributes object
            screen_permissions = role_permissions['all_attributes'].get('screen_permissions', None)
            
            # Ensure screen_permissions is not empty before returning
            if not screen_permissions:
                return {'statusCode': 404, 'body': f'Screen permissions not found for  {user['all_attributes']['email']}'}
            
            # Prepare the final response
            user_details = {
                "email": user['all_attributes']['email'],
                "first_name": user['all_attributes']['first_name'],
                "last_name": user['all_attributes']['last_name'],
                "role_name": role_name,
                "role_id":role_id,
                "screen_permissions": screen_permissions
            }
        
            return {'statusCode': 200, 'body': user_details}
        
        except Exception as err:
            exc_type, exc_obj, tb = sys.exc_info()
            f_name = tb.tb_frame.f_code.co_filename
            line_no = tb.tb_lineno
            return {'statusCode': 400, 'body': 'User with this email permissions does not exist'}