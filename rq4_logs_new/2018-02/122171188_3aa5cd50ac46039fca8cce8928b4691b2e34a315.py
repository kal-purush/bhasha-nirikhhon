from datetime import datetime
import database_common

DT_LENGTH = 19




@database_common.connection_handler
def insert_new_user(cursor, user_name, pwd_hash, enemy):
    dt = str(datetime.now())[:DT_LENGTH]
    cursor.execute("""
                      INSERT INTO users
                      VALUES (default, %(dt)s, %(user_name)s, %(pwd_hash)s, NULL, %(enemy)s);
                      """,
                   {'dt': dt, 'user_name': user_name, 'pwd_hash': pwd_hash, 'enemy': enemy})




@database_common.connection_handler
def get_user_info_from_db(cursor, username):
    cursor.execute("""
                      SELECT * FROM users 
                      WHERE username = %(username)s;
                      """,
                   {'username': username})
    user = cursor.fetchone()
    return user