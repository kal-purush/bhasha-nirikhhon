#!/usr/bin/env python
# coding=utf-8
import sys
from vm_orm import *

db_config = {
    'dbversion': 'mysql',
    'host': '172.29.152.15',
    'user': 'root',
    'passwd': 'hitnslab',
    'db': 'VMdb',
}


class Database:

    def __init__(self):
        engine = create_engine('%s://%s:%s@%s/%s' % (db_config['dbversion'],
            db_config['user'],
            db_config['passwd'],
            db_config['host'],
            db_config['db']), echo=False)
        self.metadata = Base.metadata
        db_session = sessionmaker(bind=engine)
        self.session = db_session()

    def insert_to_db(self, args):
        # vms_table = Table('vmtools_vm', self.metadata, autoload=True)
        try:      
            new_vm = VM(username=args['user'], VMName=args['dst_disk'], STATE=0, CPU=args['cpu'], Memory=args['memory'], Hard_disk=100, VNC=args['vnc'])
            self.session.add(new_vm)
            self.session.commit()
            print 'Insert to database.'
        except Exception, e:
            print 'Error: ', e
            sys.exit()

    def delete_from_db(self, username, dst_disk):
        try: 
            vm = self.session.query(VM).filter_by(VMName=dst_disk, username=username).one()
            self.session.delete(vm)
            self.session.commit()
            print 'delete from db'
        except Exception, e:
            print e
            sys.exit()

    def has_user(self, username):
        # user_table = Table('auth_user', self.metadata, autoload=True)
        client = self.session.query(Auth_User).filter_by(username=username).first()
        if client:
            return True
        else:
            return False

    def has_image(self, image_name):
        # Table('vmtools_osversion', self.metadata, autoload=True)
        image = self.session.query(OSVersion).filter_by(Version=image_name).first()
        if image:
            return (True, image)
        else:
            all_image = self.session.query(OSVersion).all()
            return (False, image)

    def finish(self):
        self.session.close()