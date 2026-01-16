from database import db, Orders

def modify(order):
  order.Order_size = 1000
  db.session.commit()

def add(order):
    db.session.add(order)
    db.session.commit()

def delete(order):
    db.session.delete(order)
    db.session.commit()

def main():
    db.create_all()

    #add order to database
    Order = Orders(API_key='abcdefghijklmnopqrstuvxy', Order_id='00000000-0000-0000-0000-000000000000', Order_size=1, Order_side='Buy', Order_symbol="XBTUSD")
    add(Order)

    #read database, and print order size
    order1 = Orders.query.first()
    print("Order size of the queried object before the modification is %d\n" % order1.Order_size)

    #modify order size
    modify(order1)
    #print order size again
    order1 = Orders.query.first()
    print("Order size of the queried object after the modification is %d\n" % order1.Order_size)

    ordertest = Orders.query.first()
    delete(ordertest)

    #Deletion, print is None which means that the database is empty
    ordertest = Orders.query.first()
    print("Print of the queried object:")
    print(ordertest)
    print("It is None because the database is empty after deletion of the single model")

if __name__ == '__main__':
    main()