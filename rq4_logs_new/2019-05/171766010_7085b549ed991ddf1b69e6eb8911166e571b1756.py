from lecopain import db
from datetime import datetime

class Customer(db.Model)        : 
    __tablename__               = 'customers'
    id                          = db.Column(db.Integer, primary_key                             = True)
    firstname                   = db.Column(db.String(50), nullable                             = False)
    lastname                    = db.Column(db.String(50), nullable                             = False)
    address                     = db.Column(db.String(200))
    cp                          = db.Column(db.String(20))
    city                        = db.Column(db.String(50))
    email                       = db.Column(db.String(200), nullable                            = False)
    created_at                  = db.Column(db.DateTime, nullable                               = False, default = datetime.utcnow)
    orders                      = db.relationship('CustomerOrder', backref                      = 'owner', lazy  = True)

    def __repr__(self)          : 
        return "Customer('{self.firstname}','{self.lastname}','{self.email}')"

    def to_dict(self)           : 
        orders_dict             = []
        for order in self.orders: 
            orders_dict.append(order.to_dict())
        return {
            'id'                : self.id,
            'firstname'         : self.firstname,
            'lastname'          : self.lastname,
            'address'           : self.address,
            'cp'                : self.cp,
            'city'              : self.city,
            'email'             : self.email,
            'orders'            : orders_dict
        }
class CustomerOrder(db.Model)   : 
    __tablename__               = 'customer_orders'
    id                          = db.Column(db.Integer, primary_key                             = True)
    title                       = db.Column(db.String(50), nullable                             = False)
    delivery_dt                 = db.Column(db.DateTime)
    created_at                  = db.Column(db.DateTime, nullable                               = False, default = datetime.utcnow)
    customer_id                 = db.Column(db.Integer, db.ForeignKey('customers.id'), nullable = False)
    status                      = db.Column(db.String(20), nullable                             = False)

    def to_dict(self)           : 
        return {
            'id'                : self.id,
            'title'             : self.title,
            'delivery_dt'       : self.delivery_dt,
            'status'            : self.status
        }

