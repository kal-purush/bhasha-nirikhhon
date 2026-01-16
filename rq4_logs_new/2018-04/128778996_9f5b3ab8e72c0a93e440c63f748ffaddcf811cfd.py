#!/usr/bin/env python
# import MySQLdb
#
# db = MySQLdb.connect(host="localhost",    # your host, usually localhost
#                      user="root",         # your username
#                      passwd="root",  # your password
#                      db="k8sql")        # name of the data base
# cur = db.cursor()
# dict_ip = {}
# dict_status = {}
# dict_ip['sth'] = '5.1.1.1'
# dict_status['sth'] = 'running'
# dict_ip['sth2'] = '5.2.2.2'
# dict_status['sth2'] = 'running'


def update_ip_status(dict_ip, dict_status, cur, db):

    for key in set(dict_ip.keys()) & set(dict_status.keys()):
        cur.execute("""UPDATE nodes SET nodes.public_ip = %s, nodes.status = %s WHERE nodes.id = %s""", (dict_ip[key], dict_status[key], key))
        db.commit()
    cur.execute("SELECT * FROM nodes")
    for row in cur.fetchall():
        print row


#update_ip_status(dict_ip, dict_status, cur)
# db.close()