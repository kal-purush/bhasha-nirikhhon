#!/usr/bin/python

import MySQLdb
class Locations:

    def get_locations(self):
        #Open database connection
        db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="secret", db="restaurant", port=33306)
        cursor = db.cursor()

        get_all = "SELECT * FROM location;"

        location_from_db = []

        try:
            # Execute the SQL command
            cursor.execute(get_all)

            # Fetch all the rows in a list of lists.

            results = cursor.fetchall()

            for row in results:
                address = row[0]
                type = row[1]
                hours = row[2]
                manager = row[3]
                location_from_db.append({'address': address, 'type': type, 'hours': hours, 'manager': manager})
        except:
            return 0

        db.close()

        return location_from_db

    def add_location(self, address, type, hours, manager):
        location_instance = Locations()
        current_locations = location_instance.get_locations()

        # check to make sure a tuple with that primary key doesn't already exist
        for location in current_locations:
            if address == location["address"]:
                return 0

        add_command = "INSERT INTO restaurant.location(address, type, hours, manager) VALUES ('" + address + "', '" + type + "', '" + hours + "', '" + manager + "');"

        db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="secret", db="restaurant", port=33306)
        cursor = db.cursor()

        try:
            # Execute the SQL command
            cursor.execute(add_command)
            db.commit()
            return {'address': address, 'type': type, 'hours': hours, 'manager': manager }
            db.close()
        except:
            db.rollback()
            return 1

    def delete_location(self, address):
        location_instance = Locations()
        current_locations = location_instance.get_locations()

        # check to make sure a tuple with that primary key doesn't already exist
        for location in current_locations:
            if address == location["address"]:
                # remove
                add_command = "DELETE FROM restaurant.location WHERE address='" + address + "';"

                db = MySQLdb.connect(host="127.0.0.1", user="root", passwd="secret", db="restaurant", port=33306)
                cursor = db.cursor()

                try:
                    # Execute the SQL command
                    cursor.execute(add_command)
                    db.commit()
                    db.close()
                    return {'address': address}
                except:
                    db.rollback()
                    return 1