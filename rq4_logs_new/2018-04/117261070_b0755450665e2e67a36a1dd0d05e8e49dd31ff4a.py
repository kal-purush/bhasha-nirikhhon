#!/usr/bin/python

import MySQLdb


class Login:

    def login_success(self, name, number):
        # Open database connection
        db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="secret", db="restaurant", port=33306)
        cursor = db.cursor()

        get_employee = "SELECT * FROM restaurant.employee WHERE id='" + number + "';"

        try:
            # Execute the SQL command
            cursor.execute(get_employee)

            # Fetch all the rows in a list of lists.
            results = cursor.fetchall()

            if len(results) > 1 or len(results) is 0:
                db.close()
                return False
            elif name == results[0][0] and int(number) == int(results[0][1]):
                db.close()
                return True
        except:
            db.close()
            return False
