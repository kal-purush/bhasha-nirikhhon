import sqlalchemy
import sqlite3
from cryptography.fernet import Fernet


def get_key():
        conn = sqlite3.connect('vault.db')
        curs = conn.cursor()
        curs.execute('SELECT password FROM master m where m.id = 2')
        rows = curs.fetchall()
        for row in rows:
                return row[0]


key = get_key().encode('utf-8')
cipher_suite = Fernet(key)


def encrypt(data):
    encoded_text = cipher_suite.encrypt(str(data).encode('utf-8'))
    return encoded_text


def decrypt(data):
    decoded_text = cipher_suite.decrypt(data)
    return decoded_text


def get_master_login():
        conn = sqlalchemy.create_engine('sqlite:///vault.db')
        sql = 'SELECT login FROM master m WHERE m.id = 1'
        rows = conn.execute(sql)
        for _row in rows:
                return decrypt(_row[0].encode('utf-8')).decode('utf-8')


def get_master_pass():
        conn = sqlalchemy.create_engine('sqlite:///vault.db')
        sql = 'SELECT password FROM master m WHERE m.id = 1'
        rows = conn.execute(sql)
        for _row in rows:
                return decrypt(_row[0].encode('utf-8')).decode('utf-8')


def get_passwords():
        conn = sqlite3.connect('vault.db')
        curs = conn.cursor()
        curs.execute('SELECT service FROM passwords')
        service = curs.fetchall()
        curs.execute('SELECT login FROM passwords')
        login = curs.fetchall()
        curs.execute('SELECT password FROM passwords')
        password = curs.fetchall()
        curs.close()
        conn.close()
        return service, login, password

# encrypted_data = get_passwords()[0][0].encode('utf-8')
# print(decrypt(encrypted_data).decode('utf-8'))


services, logins, passwords = get_passwords()
outputs = []


def output():
        output_count = 0
        while len(outputs) < len(services):
                output_dict = {}
                output_dict['Resource_name'] = decrypt(services[output_count][0].encode('utf-8')).decode('utf-8')
                output_dict['Login'] = decrypt(logins[output_count][0].encode('utf-8')).decode('utf-8')
                output_dict['Password'] = decrypt(passwords[output_count][0].encode('utf-8')).decode('utf-8')
                outputs.append(output_dict)
                output_count += 1
        for output_ in outputs:
                print(output_)


def check_master_password():
        checker_list = []
        conn = sqlalchemy.create_engine('sqlite:///vault.db')
        sql = 'SELECT password FROM master m WHERE m.id = 1;'
        rows = conn.execute(sql)
        for row in rows:
                checker_list.append(row[0])
        return checker_list[0]


def create_master_password(new_master_password):
        conn = sqlalchemy.create_engine('sqlite:///vault.db')
        sql = 'UPDATE master SET password=? WHERE id = 1'
        rows = conn.execute(sql, new_master_password)
        print('Your password were saved!')