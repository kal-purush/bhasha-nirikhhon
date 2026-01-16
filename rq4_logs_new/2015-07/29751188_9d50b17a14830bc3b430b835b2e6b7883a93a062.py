import logging as log

connection = None
cursor = None
driver = None


def connect(address, username, password, database, unix_socket='/tmp/mysql.sock'):
	"""
	Connect to the database
	"""

	global connection
	global cursor
	global driver

	connection = driver.connect(address, username, password, database, unix_socket=unix_socket)
	with connection:
		cursor = connection.cursor()
		log.info('Connected to database')


def init_driver():
	"""
	Use native MySQLdb driver, or fall back on pymysql
	Must be called before connect()
	"""

	global driver

	try:
		import MySQLdb
		log.info('Using MySQLdb')
		driver = MySQLdb
		return MySQLdb
	except ImportError:
		log.info('MySQLdb not found')
		try:
			import pymysql
			log.info('Using pymysql')
			driver = pymysql
			return pymysql
		except ImportError:
			raise Exception('No database driver found')