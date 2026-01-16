import sys
import os
import json
import mysql.connector

file_dir = 'public/public_images/2018/Genap'
mydb = mysql.connector.connect(
    host='localhost',
    user='testUser',
    passwd='bose12345*',
    database='IBDB'
)

def process_file(filename):
    with open(filename,'r') as myfile :
        data = myfile.read()
    obj = json.loads(data)
    #print "-----------------------------------"
    record_collector = []
    for record in obj :
        if ('Id_Photo' in record) :
            '''
             ada id photo jadi bisa cari attendance
             1. cari dulu attendance dengan Id_Photo tersebut
             2. get Id_student
             3. query info student
             4. put into json
            '''
            db_cursor = mydb.cursor(buffered=True)
            sql = "select * from attendances where Id_Photo = "  + record["Id_Photo"]
            db_cursor.execute(sql)
            result = db_cursor.fetchone() #yang udah absen
            if (result) :
                (Id_att, Id_Schedule, Id_Photo, Id_Student, Is_Valid, created_at, update_at) = result
                sql = "select UserId,Name from users where Id = " + str(Id_Student)
                db_cursor.execute(sql)
                (NRP, Name) = db_cursor.fetchone()
                newJsonObject = {
                    'NRP' : NRP,
                    'Name' : Name,
                    'X' : record['X'],
                    'Y' : record['Y'],
                    'Width' : record['Width'],
                    'Height' : record['Height'],
                    'Id_Photo': record['Id_Photo']
                }
                record_collector.append(newJsonObject)
            else : #yang belum absen
                newJsonObject = {
                    'NRP' : '',
                    'Name' : '',
                    'X' : record['X'],
                    'Y' : record['Y'],
                    'Width' : record['Width'],
                    'Height' : record['Height'],
                    'Id_Photo': record['Id_Photo']
                }
                record_collector.append(newJsonObject)
    print(json.dumps(record_collector))
    with open(filename,'w') as myfile :
        json.dump(record_collector,myfile,indent=4, sort_keys=True);
    return 1

def process_file_To_list(filename):
    with open(filename,'r') as myfile :
        data = myfile.read()
    obj = json.loads(data)
    print "-----------------------------------"
    for record in obj :
        if (("NRP" in record) and ("Name" in record)):
            print record["NRP"] + " " + record["Name"]
    return 1


def list_files(file_dir) :
    r = []
    count = 0
    for root, dirs, files in os.walk(file_dir) :
        for name in files :
            if ((name.endswith('.json')) and (name != 'meta.json')):
                file_total_path = os.path.join(root,name)
                r.append(file_total_path)
                #print file_total_path
                process_file(file_total_path)
                count = count+1
    print "number of files  :  " + str(count)
    return r


list_files(file_dir)