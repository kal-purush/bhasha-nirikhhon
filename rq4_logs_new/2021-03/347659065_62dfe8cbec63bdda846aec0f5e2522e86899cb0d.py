import json
from json import JSONEncoder
from flask import Flask, jsonify, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy 

app = Flask(__name__)
# mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:hejsan123@localhost/testdatabasen'
db = SQLAlchemy(app)

class Bil2(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    namn = db.Column(db.String(80), unique=False, nullable=False)
    modell = db.Column(db.String(20), unique=False, nullable=False)
    year = db.Column(db.Integer, unique=False, nullable=False)

class Mat2(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    namn = db.Column(db.String(80), unique=False, nullable=False)
    pris = db.Column(db.Integer, unique=False, nullable=False)
    typ = db.Column(db.String(1), unique=False, nullable=False) #Vegetarisk, Kött,

    def serialize(self):
        return {
            'typ': self.typ,
            'namn': self.namn,
            'pris': self.pris,
            'id': self.id,
        }    


db.create_all()


# v1 = Mat2(namn="Hamburgare", typ="K", pris=99  )
# v2 = Mat2(namn="Pannkakor", typ="V", pris=20  )

# db.session.add(v1)
# db.session.add(v2)
# db.session.commit()


@app.route('/mat2/<int:id>', methods=['GET'])
def showMat(id):    
    m = Mat2.query.filter_by(id=id).one()
    return jsonify(m.serialize())



@app.route('/mat2', methods=['GET'])
def get_mat():
    q = Mat2.query.all()
    return jsonify([b.serialize() for b in q])

app.run()


# q = Mat2.query.filter_by(typ='V').all()
# for x in q:
#     print(x)



# while(True):
#     print("Skapa en ny!!")
#     namn = input("Namn")
#     pris = int(input("pris"))
#     typ = input("typ")
#     v1 = Mat2(namn=namn, pris=pris, typ=typ)
#     db.session.add(v1)
#     db.session.commit()

# db.create_all()

# v1 = Bil(namn="Volvo", modell="XC60", year=2016)
# v2 = Bil(namn="Saab", modell="V4", year=1973)

# db.session.add(v1)
# db.session.add(v2)
# db.session.commit()
