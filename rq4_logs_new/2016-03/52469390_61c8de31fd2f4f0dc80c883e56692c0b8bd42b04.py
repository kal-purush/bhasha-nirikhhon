import sqlite3
import urllib.request
import os
from datetime import datetime
import time

VALID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890./ "

class Vp():
    def __init__(self, website, sid, database):
        """
        initialize Variables and the database
        """
        self.__website = website
        self.__databaseFile = database
        createDatabase = not os.path.exists(self.__databaseFile)
        self.__database = sqlite3.connect(self.__databaseFile)
        self.__cursor = self.__database.cursor()
        self.__lastChange = "" # date and time, when the last change has happend
        self.__sid = sid
        if (createDatabase):
            self.__createDatabase()


    def __checkInput(self, string):
        """
        check if the string only got valid characters
        """
        for char in string:
            if (not char in VALID_CHARS):
                return False
        return True


    def __createDatabase(self):
        """
        Create the tables in the database
        """
        sql_command = """
            CREATE TABLE user ( 
                userId INTEGER PRIMARY KEY, 
                username VARCHAR(32) DEFAULT 0,
                sid VARCHAR(32) DEFAULT 0,
                joining TIMESTAMP);
        """
        self.__cursor.execute(sql_command)

        sql_command = """
            CREATE TABLE entry (
                entryId INTEGER PRIMARY KEY,
                date DATE,
                day CHAR(10),
                hour INTEGER,
                course CHAR(10),
                lesson CHAR(5),
                change CHAR(64));
        """
        self.__cursor.execute(sql_command)

        sql_command = """
            CREATE TABLE course (
                userId INTEGER NOT NULL,
                course CHAR(10) NOT NULL,
                FOREIGN KEY(userId) REFERENCES user(userId) ON UPDATE CASCADE,
                PRIMARY KEY (userId, course));
        """
        self.__cursor.execute(sql_command)

        self.__database.commit()

    def isAuthorised(self, userId):
        """
        Returns if the given User is Authorised
        """
        sql_command = """
            SELECT userId
            FROM user
            WHERE userId = ?
        """
        self.__cursor.execute(sql_command, (userId,))
        return (self.__cursor.fetchone() != None)

    def addUser(self, userId):
        """
        Adding a user to the database
        """
        sql_command = """
            INSERT INTO user (userId, joining)
            VALUES (?, ?)
        """
        self.__cursor.execute(sql_command, (userId, datetime.now()))
        self.__database.commit()


    def checkUser(self, userId, url):
        """
        checks whether a user is valid and add this user to the database 
        """
        sid = url[url.find("=")+1:]
        if (not self.__checkInput(sid)):
            print("Authentification failed: input='"+url+"'")
            return ("Anmeldung fehlgeschlagen.") #TODO: language file
        page = urllib.request.urlopen(self.__website.format(sid = sid))\
                .read().decode('cp1252')
        
        phrase = "Sie sind angemeldet als <strong>"
        index = page.find(phrase)
        if (index > 0):
            index += len(phrase)
            lenUsername = 0
            while (page[index + lenUsername] != "<"):
                lenUsername += 1

            username = page[index:index+lenUsername]

            sql_command = """
                SELECT username
                FROM user
                WHERE userId = ?
            """
            self.__cursor.execute(sql_command, (userId,))
            prevUsername = self.__cursor.fetchone()

            if (prevUsername == None):
                print("Authentification successful id="+str(userId)\
                        +" username='"+username+"'")

                sql_command = """
                    INSERT INTO user (userId, username, sid, joining)
                    VALUES (?, ?, ?, ?, ?) 
                """
                self.__cursor.execute(sql_command,\
                        (userId, username, sid, datetime.now()))
                
                self.__database.commit()
                return ("Anmeldung erfolgreich als " + username)#TODO

            else:
                prevUsername = prevUsername[0]
                print("Reauthentification successful id="+str(userId)\
                        +" ('"+prevUsername+"'->'"+username+"')")
                if (prevUsername != username):
                    sql_command == """
                        UPDATE user
                        SET username = ?
                        WHERE userId = ?
                    """
                    self.__cursor.execute(sql_command, (username, userId))
                    self.__database.commit()

                return ("Anmeldung erfolgreich als {username}"\
                        .format(username=username))

        else:
            print("Authentification failed id=" + str(userId)+ " sid='"+sid+"'")
            return ("Anmeldung fehlgeschlagen.") #TODO: language file
                


    def addUserSubjects(self, userId, subjects):
        """
        add all given subjects to the user
        """
        if (not self.isAuthorised(userId)):
            return ("Anmeldung erforderlich")

        subjects = subjects.split(",")
        added = 0
        failed = 0
        equal = 0

        # Check input
        for i in range(len(subjects)):
            subjects[i] = subjects[i].strip()
            if (not self.__checkInput(subjects[i])):
                del subjects[i]
                failed += 1

        subjects = list(set(subjects)) #Remove dublicates

        # Get current subjects
        sql_command = """
            SELECT course
            FROM course
            WHERE userId = ?
        """
        self.__cursor.execute(sql_command, (userId,))
        curSubjects = self.__cursor.fetchall()
        for i in range(len(curSubjects)):
            curSubjects[i] = curSubjects[i][0]

        #insert subjects into the database
        sql_command = """
            INSERT INTO course (userId, course)
            VALUES (?, ?)
        """
        addedSubjects = str()
        for elem in subjects:
            if (elem in curSubjects):
                equal += 1
                subjects.remove(elem)
            else:
                self.__cursor.execute(sql_command, (userId, elem))
                added += 1
                addedSubjects += elem + ", "

        self.__database.commit()
        print("Subject added userId="+str(userId)+" added="+str(added)\
                +" equal="+str(equal)+" failed="+str(failed))
        
        if (added == 0):
            return ("Keine Kurse dazugekommen.")

        elif (added == 1):
            return ("Dazugekommener Kurs: " + addedSubjects[:-2])

        else:
            return ("Dazugekommene Kurse: " + addedSubjects[:-2])


    def delUserSubjects(self, userId, subjects):
        """
        delete all given subjects from the user
        """
        subjects = subjects.split(",")
        for i in range(len (subjects)):
            subjects[i] = subjects[i].strip()
            if (not self.__checkInput(subjects[i])):
                del subjects[i]

        subjects = list(set(subjects)) # Remove dublicates
        
        sql_command = """
            DELETE FROM course
            WHERE userId = ?
            AND course = ?
        """
        print(subjects)

        for subject in subjects:
            self.__cursor.execute(sql_command, (userId, subject))
        self.__database.commit()

        return ("Ok, alle gegebenen Kurse entfernt")
        


    def resetUserSubjects(self, userId):
        """
        deletes all subjects from the user
        """
        sql_command = """
            DELETE FROM course
            WHERE userId = ?
        """
        self.__cursor.execute(sql_command, (userId,))
        self.__database.commit()


    def getUserInfo(self, userId):
        """
        returns the info of the user as a message string Info includes
        all subjects of the user and an information
        """
        sql_command = """
            SELECT course 
            FROM course
            WHERE userId = ?
        """
        subjects = ""
        self.__cursor.execute(sql_command, (userId,))
        for (subject,) in self.__cursor.fetchall():
            subjects += "\n" + subject 

        return ("Deine Kurse sind: "+ subjects)
        



    def getUserStatus(self, userId):
        """
        returns the current vp Status from the user as a message string
        """
        sql_command = """
            SELECT date, day, hour, course, lesson, change 
            FROM course
            NATRUAL JOIN entry
            WHERE userId = ?
            AND date >= ?
        """
        self.__curosr.execute(sql_command,\
                (userId, datetime.date(datetime.now())))
        print(self.__cursor.fetchall())
        for (date, day, hour, course, lesson, change)\
                in self.__curosr.fetchall():
            pass

    def getUpdates(self):
        """
        returns the changes of the vp for every user in the form
        [(userId, "message"), (userId2, "message"), ...]
        """
        page = urllib.request.urlopen(self.__website\
                .format(sid=self.__sid)).read().decode("cp1252")
        entries = []
        
        #TODO: check if the date has changed

        page = page[page.index("</p>")+4:] #delete first paragraph
        for day in (page.split("</table>")[:-1]):
            date = ""
            weekday = ""
            
            # Locate the date
            start = 0
            end = 0
            for char in day:
                if (char == "<"):
                    start += 1
                    if (date):
                        break
                elif (char == ">"):
                    end += 1
                elif (start == end):
                    if (char.strip() != ""):
                        if (char in "1234567890."):
                            date += char
                        else:
                            weekday += char

            date = date.split(".")
            date = "{year}-{month}-{day}"\
                    .format(year = date[2], month = date[1], day = date[0])
            wks = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag"]
            for wk in wks:
                if (wk.lower() in weekday.lower()):
                    weekday = wk

            curEntry = []


            


if (__name__ == "__main__"):
    vp = Vp("http://archenhold.de/api/vp.php?sid={sid}",\
            "eS6aiOHXrJ6NoVS2spANCrfcowyFO5H3",\
            "telegramBot.db")

    #vp.checkUser(1, "")
    #vp.checkUser(1, "eS6aiOHXrJ6NoVS2spANCrfcowyFO5H3")

    #print(vp.addUserSubjects(2, "test1, test2, test3, test1, h!2"))
    #print(vp.getUserInfo(1))
    #print(vp.delUserSubjects(2, "test1, test2"))
    #print(vp.getUserInfo(1))
    print(vp.getUpdates())
