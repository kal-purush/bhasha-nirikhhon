from flask import request
from Models import storage


#f"SELECT * FROM Profile WHERE ageRange = '{user_data['ageRange']}' AND \
            #    religion = '{user_data['religion']}' AND gender = '{user_data['gender']}' LIMIT 20"  # Fetch the first 20 users
         #   if 'last_index' in user_data and user_data['last_index'] is not None and user_data['last_index'] != 0:
       #         query += f" OFFSET {user_data['last_index']}"

def all_messages():
    try:
        name = request.args.get('name')
        query = f"SELECT * FROM Messages WHERE sent_by ='{name}' OR sent_to = '{name}'"
        messages = storage.take_query(query)
        return messages
    except Exception as e:
        print(e)
        return "something is wrong", 400