
import mysql.connector
from mysql.connector.cursor import MySQLCursorPrepared
import config

class SQLClient:
    def __init__(self):
        #connection info
        self.usr = config.mysql['user']
        self.pwd = config.mysql['password']
        self.hst = config.mysql['host']
        self.dab = 'mdulin2_DB'

    def test(self):
        """
        A function to test the SQL code in
        """

        self.get_plants() # in order to call a function inside of a class use "self.function_name"
        self.disply_table('O')

    def get_plants(self):
        """
        Gets all of the plants
        Returns:
            A list of all the plant id's
        """

        try:
            con = mysql.connector.connect(user=self.usr,password=self.pwd, host=self.hst,
                                          database=self.dab) #connection

            rs = con.cursor(cursor_class=MySQLCursorPrepared) #changing the type of cursor
            query = 'SELECT plantID,locationID FROM Plant;' #the query itself
            rs.execute(query) #executing the query
            # create a result set
            plants_list = []
            text = ""
            #iterating through the database
            for (plantID,locationID) in rs:
                text = '{},{}'.format(plantID,locationID)
                plants_list.append(text[0])

            rs.close() #close the cursor
            con.close() #close the connection
            return plants_list
        except mysql.connector.Error as err:
            print("come on! ")

    def disply_table(self, table_start):
        """
        Displays the table
        Args:
            table_start: The table to be displayed; there are five possible options...
            O-PlantOwnership
            W-WaterEvent
            U-Users
            L-Location
            T-PlantType
        """

        # create a connection
        con = mysql.connector.connect(user=self.usr,password=self.pwd, host=self.hst,
                                      database=self.dab)
        rs = con.cursor(cursor_class=MySQLCursorPrepared)
        if(table_start == 'O'):
            table = "PlantOwnership"
        elif(table_start == 'W'):
            table = "WaterEvent"
        elif(table_start == 'U'):
            table = "Users"
        elif(table_start == "L"):
            table = "Location"
        elif(table_start == "T"):
            table = "PlantType"
        else:
            assert(False)

        statement = 'SELECT * FROM %s' % (table) # %s is the placeholder, table is the variable
        #might want to turn this into a prepared statement?
        rs.execute(statement)

        for pull in rs:
            print pull #displays in element in the database pull
        """
        for (cname,r1,r2,r3) in rs:
            text += '{},{},{},{}'.format(cname,r1,r2,r3)
            text+='\n'
        """
        rs.close()
        con.close()


if __name__ == '__main__':
    pass