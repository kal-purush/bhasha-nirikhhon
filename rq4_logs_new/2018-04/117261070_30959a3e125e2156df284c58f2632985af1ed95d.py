#!/usr/bin/python

import MySQLdb


class Employees:

    def get_employees(self):
        # Open database connection
        db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="secret", db="restaurant", port=33306)
        cursor = db.cursor()

        get_all = "SELECT * FROM employee;"

        employees_from_db = []

        try:
            # Execute the SQL command
            cursor.execute(get_all)

            # Fetch all the rows in a list of lists.
            results = cursor.fetchall()

            for row in results:
                name = row[0]
                id = row[1]
                wage = row[2]
                status = row[3]
                location = row[4]
                employees_from_db.append({'name': name, 'id': id, 'wage': wage, 'status': status, 'location': location})
        except:
            return 0

        db.close()

        return employees_from_db

    def add_employee(self, name, id, wage, status, location):
        employee_instance = Employees()
        current_employees = employee_instance.get_employees()

        # check to make sure a tuple with that primary key doesn't already exist
        for employee in current_employees:
            if id == employee["id"]:
                return 0

        add_command = "INSERT INTO restaurant.employee(name, id, wage, status, location) VALUES ('" + name + "', '" + id + "', '" + str(
            wage) + "', '" + status + "', '" + location + "');"

        db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="secret", db="restaurant", port=33306)
        cursor = db.cursor()

        try:
            # Execute the SQL command
            cursor.execute(add_command)
            db.commit()
            return {'name': name, 'id': id, 'wage': wage, 'stauts': status, 'location': location}
            db.close()
        except:
            db.rollback()
            return 1

    def delete_employee(self, id):
        employee_instance = Employees()
        current_employees = employee_instance.get_employees()

        # check to make sure a tuple with that primary key doesn't already exist
        for employee in current_employees:
            if int(id) == int(employee["id"]):
                # remove
                add_command = "DELETE FROM restaurant.employee WHERE id='" + str(id) + "';"

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
