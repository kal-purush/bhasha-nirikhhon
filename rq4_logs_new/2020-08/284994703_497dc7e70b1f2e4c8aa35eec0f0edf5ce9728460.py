import pyodbc
import mysql.connector

def stringify(val):
	return "'" + val + "'"

class MSSQLDB:
	def __init__(self, conn_string, database):
		self.database = database
		self.db = pyodbc.connect(conn_string)
		self.cursor = self.db.cursor()

	def insert_data(self, data):
		first = "insert into {} (".format("email_table")
		second = "values ("
		for key, val in data.items():
			if val != '-':
				first += key.lower().replace(' ', '_') + ', '
				if key != 'attachment_present':
					second += stringify(val.replace("'", "")) + ', '
				else:
					second += str(val) + ', ' 
		first = first.strip()[:-1] + ')'
		second = second.strip()[:-1] + ')'
		query = first + " " + second
		try:
			self.cursor.execute(query)
		except Exception as err:
			print (err)
			return False, 'there was an error'
			pass
		self.db.commit()
		return True, 'success'

class MySQLDB:

	def __init__(self, host, user, passwd, database, table):
		self.table = table
		self.db = mysql.connector.connect(
			host=host, 
			user=user, 
			passwd=passwd, 
			database=database)
		self.cursor = self.db.cursor()
		print ('[+] Connection to DB successful')

	def insert_data(self, data):
		first = "insert into {} (".format(self.table)
		second = "values ("
		for key, val in data.items():
			if val != '-':
				first += key.lower().replace(' ', '_') + ', '
				if key != 'attachment_present':
					second += stringify(val.replace("'", "")) + ', '
				else:
					second += str(val) + ', ' 
		first = first.strip()[:-1] + ')'
		second = second.strip()[:-1] + ')'
		query = first + " " + second
		try:
			self.cursor.execute(query)
		except mysql.connector.errors.IntegrityError:
			return False, 'duplicate entry'
			pass
		self.db.commit()
		return True, 'success'