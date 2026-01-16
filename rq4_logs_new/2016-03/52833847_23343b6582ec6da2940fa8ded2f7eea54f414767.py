from abc import ABCMeta, abstractmethod
import mysql.connector

cnx = mysql.connector.connect(database='...')
cursor = cnx.cursor()

class User(metaclass = ABCMeta):

	id = 0
	name = ""
	dob = ""
	email = ""
	password = ""
	phone = ""
	about = ""
	vhours = 0
	uhours = 0
	neighborhood = ""
	interests = ...
	skills = …
	gender = ""
	education = ""
	availability = ""
	last_activity = ""
	events …

	@abstractmethod
	def editName(self, name):
		"UPDATE "
		"SET name = name"

	@abstractmethod
	def editDOB(self, dob):
		"UPDATE "
		"SET dob = dob"

	@abstractmethod
	def editGender(self, gender):
		"UPDATE "
		"SET dob = dob"

	@abstractmethod
	def editBio(self, bio):
		"UPDATE "
		"SET dob = dob"

	@abstractmethod
	def editInterests(self, interests):
		"UPDATE "
		"SET interests = interests"

	@abstractmethod
	def editEmail(self, email):
		"UPDATE "
		"SET email = email"

	@abstractmethod
	def editPassword(self, password):
		"UPDATE "
		"SET password = password"

	@abstractmethod
	def editPhone(self, phone):
		"UPDATE "
		"SET phone = phone"

	@abstractmethod
	def editEducation(self, education):
		"UPDATE "
		"SET education = education"

	@abstractmethod
	def logIn(self, email, password):
		if("SELECT email"
			"FROM "
			"WHERE name = self.name" = email):
			"SELECT password"
			"FROM "
			"WHERE name = self.name" = password
		else:
			False


	@abstractmethod
	def deleteProfile(self):
		"DELETE"
		"FROM"
		"WHERE id = self.id"

	@abstractmethod
	def opportunitySignUp(self, event):
		"INSERT"

	@abstractmethod
	def logHours(self, hours):
		"UPDATE"
		"SET uhours = self.uhours + hours"

	@abstractmethod
	def updateActivity(self, time):
		"UPDATE"
		"SET last_activity = time"

	@abstractmethod
	def executeFunc(func, data):
		cursor.execute(func, data)



	User.register(org_member, vol)
	cnx.commit()

	cursor.close()
	cnx.close()