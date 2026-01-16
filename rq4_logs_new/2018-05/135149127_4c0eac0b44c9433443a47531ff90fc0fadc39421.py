import sqlite3
from datetime import date, datetime


class DBHelper:
    def __init__(self, dbname="csgo_mix.db"):
        self.dbname = dbname
        self.conn = sqlite3.connect(dbname, check_same_thread=False)

    def setup(self):
        stmt_user = "CREATE TABLE IF NOT EXISTS user_telegram (id INTEGER PRIMARY KEY AUTOINCREMENT, id_telegram TEXT, first_name TEXT, alias TEXT, UNIQUE(id_telegram))"
        stmt_mix = "CREATE TABLE IF NOT EXISTS mix (id INTEGER PRIMARY KEY AUTOINCREMENT, mix_date DATE DEFAULT CURRENT_TIMESTAMP, UNIQUE(mix_date))"
        stmt_mix_user = "CREATE TABLE IF NOT EXISTS mix_user (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, mix_id INTEGER NOT NULL, FOREIGN KEY(mix_id) REFERENCES mix(id), FOREIGN KEY(user_id) REFERENCES user_telegram(id), UNIQUE(user_id, mix_id))"

        self.conn.execute(stmt_user)
        self.conn.execute(stmt_mix)
        self.conn.execute(stmt_mix_user)
        self.conn.commit()

    def get_last_mix(self):
        stmt = "SELECT *  FROM mix WHERE   ID = (SELECT MAX(id)  FROM mix)"
        return self.conn.execute(stmt)

    def get_user_or_create(self, id_telegram, first_name, alias):
        stmt_user = "SELECT EXISTS(SELECT 1 FROM user_telegram WHERE id_telegram=?)"
        user = self.conn.execute(stmt_user, (id_telegram,))
        if user is True:
            return user
        else:
            stmt = "INSERT INTO user_telegram (id_telegram, first_name) VALUES (?, ?)"
            args = (id_telegram, first_name,)
            try:
                user = self.conn.execute(stmt, args)
                self.conn.commit()
            except:
                pass
            return user

    def mix_today(self):
        today = datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
        last_mix = self.get_last_mix()
        aux_date = None
        if last_mix is True:
            for row in last_mix:
                aux_date = row[1].split(' ')[0]
            last_mix_day = datetime.strptime(aux_date, "%Y-%m-%d")
            if today.date() == last_mix_day.date():
                return True
            else:
                return False
        else:
            return False

    def create_mix(self):
        msg = None
        if self.mix_today():
            msg = 'Ya ha sido creado'
        else:
            stmt = "INSERT INTO mix (id) VALUES (NULL)"
        try:
            self.conn.execute(stmt)
            self.conn.commit()
            msg = 'Mix creado correctamente'
        except:
            pass
        return msg

    def add_item(self, id_telegram, first_name, alias):
        user = self.get_user_or_create(id_telegram, first_name, alias)
        last_mix = self.get_last_mix()
        user_id = None
        mix_id = None
        for row in user:
            user_id = row[0]
        for row in last_mix:
            mix_id = row[0]

        stmt = "INSERT INTO mix_user (user_id, mix_id) VALUES (?, ?)"
        args = (user_id, mix_id,)
        try:
            self.conn.execute(stmt, args)
            self.conn.commit()
        except:
            pass

    def delete_item(self, alias, first_name):
        # select user y mix. Ambos deben existir
        stmt = "DELETE FROM mix_user WHERE user_id = (?) AND mix_id = (?)"
        args = (alias, first_name,)
        try:
            self.conn.execute(stmt, args)
            self.conn.commit()
        except:
            print("No existe")

    def get_items(self):
        last_mix = self.get_last_mix()
        mix_id = None
        for row in last_mix:
            mix_id = row[0]
        stmt = "SELECT user_id FROM mix_user WHERE mix_id = (?)"
        list_mix = self.conn.execute(stmt, (mix_id,))
        res1 = '*MIX ' + str(date.today().strftime("%d/%m/%y")) + '*\n'
        res = res1 + '*---------------------*\n'
        cont = 0
        for row in list_mix:
            cont = cont + 1
            if cont <= 10:
                res = res + '*' + str(cont) + ".* " + str(row[0]) + "\n"
            elif cont == 11:
                res = res + '\n *SUPLENTES* \n' + '*' + str(cont) + ".* " + str(row[0]) + "\n"
                res = res + '*' + str(cont) + ".* " + str(row[0]) + "\n"
            elif cont > 11:
                res = res + '*' + str(cont) + ".* " + str(row[0]) + "\n"
        return res