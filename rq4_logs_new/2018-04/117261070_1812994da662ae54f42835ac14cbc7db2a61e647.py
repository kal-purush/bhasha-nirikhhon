#!/usr/bin/python

import MySQLdb
import datetime


class Transactions:

    def get_transactions(self):
        # Open database connection
        db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="secret", db="restaurant", port=33306)
        cursor = db.cursor()

        get_all = "SELECT * FROM TRANSACTIONS;"

        transactions_from_db = []

        try:
            # Execute the SQL command
            cursor.execute(get_all)

            # Fetch all the rows in a list of lists.
            results = cursor.fetchall()

            for row in results:
                id = row[0]
                total = row[1]
                time = row[2]
                method_of_payment=row[3]
                transactions_from_db.append({'id': id, 'total': total, 'time': time, 'method_of_payment': method_of_payment})
        except:
            return 0

        db.close()

        return transactions_from_db

    def add_transaction(self, total, method_of_payment):
        transaction_instance = Transactions()
        current_transactions = transaction_instance.get_transactions()
        time = datetime.datetime.now()

        add_command = "INSERT INTO restaurant.transaction(total, time, method_of_payment) VALUES (" + total + ", " + time + ", '" + method_of_payment + "');"
        db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="secret", db="restaurant", port=33306)
        cursor = db.cursor()

        try:
            # Execute the SQL command
            cursor.execute(add_command)
            db.commit()
            db.close()
            return {'id': id, 'total': total, 'time': time, 'method_of_payment': method_of_payment}            
        except:
            db.rollback()
            return 1

    def delete_transaction(self, id):
        transaction_instance = Transactions()
        current_transactions = transaction_instance.get_transactions()

        # check to make sure a tuple with that primary key doesn't already exist
        for transaction in current_transactions:
            if id == transaction["id"]:
                # remove
                add_command = "DELETE FROM restaurant.transaction WHERE id='" + id + "';"
                
                db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="secret", db="restaurant", port=33306)
                cursor = db.cursor()

                try:
                    # Execute the SQL command
                    cursor.execute(add_command)
                    db.commit()
                    db.close()
                    return {'id': id}
                except:
                    db.rollback()
                    return 1
