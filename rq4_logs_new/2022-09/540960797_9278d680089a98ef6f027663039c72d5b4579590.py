import pymysql
class TruthLabDatabase:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def createConnection(self):
        self.connection = pymysql.connect(host='localhost',
                                     user='truthlab_main',
                                     password='truthlab',
                                     db='truthlab_fake-news',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)

    def closeConnection(self):
        self.connection.close()

    def executeQuery(self, sql, values):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, values)
        self.connection.commit()

    def getRows(self, sql, values):
        with self.connection.cursor() as cursor:
            if (values != None):
                cursor.execute(sql, values)
            else:
                cursor.execute(sql)
        return cursor.fetchall()

    def getUserByLoginAndPassword(self, login, password):
        self.createConnection()
        rows = self.getRows('SELECT * FROM users WHERE login = %s AND password = %s', (login, password))
        self.closeConnection()
        if (len(rows) > 0):
            return rows[0]
        else:
            return None

    def getUserByLogin(self, login):
        self.createConnection()
        rows = self.getRows('SELECT * FROM users WHERE login = %s', (login))
        self.closeConnection()
        if (len(rows) > 0):
            return rows[0]
        else:
            return None

    def getUserById(self, id):
        self.createConnection()
        rows = self.getRows('SELECT * FROM users WHERE id = %s', (id))
        self.closeConnection()
        if len(rows) > 0:
            return rows[0]
        else:
            return None

    def addUser(self, login, password1, password2, username, access):
        user = self.getUserByLogin(login)
        if user != None:
            return 'Користувач з вказаним логіном вже існує'
        if password1 != password2:
            return 'Паролі не співпадають'
        self.createConnection()

        self.executeQuery('INSERT INTO users (login, password, username, access) VALUES (%s, %s, %s, %s)', (login, password1, username, access))
        self.closeConnection()
        return True

    def editUser(self, id, password1, password2, username):
        if password1 != password2:
            return 'Паролі не співпадають'
        self.createConnection()
        if len(password1) > 0:
            self.executeQuery('UPDATE users SET password = %s, username = %s WHERE id = %s', (password1, username, id))
        else:
            self.executeQuery('UPDATE users SET username = %s WHERE id = %s', (username, id))
        self.closeConnection()
        return True

    def editUserByAdmin(self, id, password, username, access):
        self.createConnection()
        self.executeQuery('UPDATE users SET password = %s, username = %s, access = %s WHERE id = %s', (password, username, access, id))
        self.closeConnection()
        return True

    def getUsers(self):
        self.createConnection()
        rows = self.getRows('SELECT * FROM users', None)
        self.closeConnection()
        if (len(rows) > 0):
            return rows
        else:
            return None

    def deleteUser(self, id):
        self.createConnection()
        self.executeQuery("DELETE FROM `users` WHERE id = %s", (id))
        self.closeConnection()

    # Themes

    def themeAdd(self, name, text):
        self.createConnection()
        self.executeQuery("INSERT INTO `themes` (`name`, `text`) VALUES (%s, %s)",
                          (name, text))
        self.closeConnection()

    def getThemes(self):
        self.createConnection()
        rows = self.getRows('SELECT * FROM themes', None)
        self.closeConnection()
        return rows

    def getTheme(self, id):
        self.createConnection()
        rows = self.getRows('SELECT * FROM themes WHERE id = %s', (id))
        self.closeConnection()
        if len(rows) > 0:
            return rows[0]
        else:
            return None

    def deleteTheme(self, id):
        self.createConnection()
        self.executeQuery("DELETE FROM`themes` WHERE id = %s", (id))
        self.closeConnection()

    def editTheme(self, id, name, text):
        self.createConnection()
        self.executeQuery("UPDATE `themes` SET name = %s, text = %s WHERE id = %s", (name, text, id))
        self.closeConnection()

    # Languages

    def languageAdd(self, name, text):
        self.createConnection()
        self.executeQuery("INSERT INTO `languages` (`name`, `text`) VALUES (%s, %s)",
                          (name, text))
        self.closeConnection()

    def getLanguages(self):
        self.createConnection()
        rows = self.getRows('SELECT * FROM languages', None)
        self.closeConnection()
        return rows

    def getLanguage(self, id):
        self.createConnection()
        rows = self.getRows('SELECT * FROM languages WHERE id = %s', (id))
        self.closeConnection()
        if len(rows) > 0:
            return rows[0]
        else:
            return None

    def deleteLanguage(self, id):
        self.createConnection()
        self.executeQuery("DELETE FROM `languages` WHERE id = %s", (id))
        self.closeConnection()

    def editLanguage(self, id, name, text):
        self.createConnection()
        self.executeQuery("UPDATE `languages` SET name = %s, text = %s WHERE id = %s", (name, text, id))
        self.closeConnection()

    # Spam

    def spamAdd(self, text):
        self.createConnection()
        self.executeQuery("INSERT INTO `spam` (`text`) VALUES (%s)", (text))
        self.closeConnection()

    def getSpamAndNoSpam(self):
        self.createConnection()
        rowsSpam = self.getRows('SELECT * FROM spam', None)
        rowsNoSpam = self.getRows('SELECT * FROM themes', None)
        self.closeConnection()
        rows = []
        for elem in rowsSpam:
            rows.append({'name' : 'спам', 'text' : elem['text']})
        for elem in rowsNoSpam:
            rows.append({'name' : 'не спам', 'text' : elem['text']})
        return rows

    def getSpam(self, id = None):
        self.createConnection()
        if id != None:
            rows = self.getRows('SELECT * FROM spam WHERE id = %s', (id))
            self.closeConnection()
            if len(rows) > 0:
                return rows[0]
            else:
                return None
        else:
            rows = self.getRows('SELECT * FROM spam', None)
        self.closeConnection()
        return rows


    def deleteSpam(self, id):
        self.createConnection()
        self.executeQuery("DELETE FROM `spam` WHERE id = %s", (id))
        self.closeConnection()

    def editSpam(self, id, text):
        self.createConnection()
        self.executeQuery("UPDATE `spam` SET text = %s WHERE id = %s", (text, id))
        self.closeConnection()

    # Fake

    def fakeAdd(self, text):
        self.createConnection()
        self.executeQuery("INSERT INTO `fake` (`text`) VALUES (%s)", (text))
        self.closeConnection()

    def getFakeAndNoFakes(self):
        self.createConnection()
        rowsFake = self.getRows('SELECT * FROM fake', None)
        rowsNoFake = self.getRows('SELECT * FROM themes', None)
        self.closeConnection()
        rows = []
        for elem in rowsFake:
            rows.append({'name' : 'фейк', 'text' : elem['text']})
        for elem in rowsNoFake:
            rows.append({'name' : 'не фейк', 'text' : elem['text']})
        return rows

    def getFake(self, id = None):
        self.createConnection()
        if id != None:
            rows = self.getRows('SELECT * FROM fake WHERE id = %s', (id))
            self.closeConnection()
            if len(rows) > 0:
                return rows[0]
            else:
                return None
        else:
            rows = self.getRows('SELECT * FROM fake', None)
        self.closeConnection()
        return rows


    def deleteFake(self, id):
        self.createConnection()
        self.executeQuery("DELETE FROM `fake` WHERE id = %s", (id))
        self.closeConnection()

    def editFake(self, id, text):
        self.createConnection()
        self.executeQuery("UPDATE `fake` SET text = %s WHERE id = %s", (text, id))
        self.closeConnection()



    def newsAdd(self, title, text, url, site, fake, theme):
        self.createConnection()
        self.executeQuery("INSERT INTO `news` (`title`, `text`, `url`, `site`, `fake`, `theme`) VALUES (%s, %s, %s, %s, %s, %s)",
                          (title, text, url, site, fake, theme))
        self.closeConnection()
