from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import os 

# init app 

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))
#DB
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir,'db.sqlite')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#Init DB
db = SQLAlchemy(app)
#Init MA
ma = Marshmallow(app)

# VM Class / model  

class Vm(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    hostname = db.Column(db.String(100), unique=True)
    address = db.Column(db.String(100), unique=True)
    label = db.Column(db.String(20))
    owner = db.Column(db.String(100))
    lock = db.Column(db.Boolean, default=False)

    def __init__(self, hostname, address, label, owner, lock):
        self.hostname = hostname
        self.address = address
        self.label = label
        self.owner = owner
        self.lock = lock

# VM schema 
class VmSchema(ma.Schema):
    class Meta:
        fields = ('hostname', 'address', 'label', 'owner', 'lock')

#init Schema 
vm_schema = VmSchema()
vms_schema = VmSchema(many=True)

#Create Vm 

@app.route('/vm', methods=['POST'])
def add_vm():
    hostname = request.json['hostname']
    address = request.json['address']
    label = request.json['label']
    owner = request.json['owner']
    lock = request.json['lock']

    new_vm = Vm(hostname, address, label, owner, lock)

    db.session.add(new_vm)
    db.session.commit()

    return vm_schema.jsonify(new_vm)
@app.route('/vm', methods=['GET'])
def get_vms():
    all_vms = Vm.query.all()
    result = vms_schema.dump(all_vms)
    return jsonify(result)

@app.route('/vm/<label>', methods=['GET'])
def get_label(label):
    all_vms = Vm.query.filter(Vm.label == label).filter(Vm.lock == False)
    result = vms_schema.dump(all_vms)
    return jsonify(result)

#run server 

if __name__ == "__main__":
  app.run(debug=True)
