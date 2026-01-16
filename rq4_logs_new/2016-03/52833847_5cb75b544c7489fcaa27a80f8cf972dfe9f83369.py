from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relation, sessionmaker
from enum import Enum

Base = declarative_base()
#replace with config setting for database
database_engine = create_engine("mysql://...")
Session.configure(bind=database_engine)



class GenderEnum(Enum):
	male = "Male"
	female = "Female"
	other = "Other / Prefer not to specify"

class PermissionsEnum(Enum):
	volunteer = "Volunteer"
	org_member = "Organization Member"
	admin = "Admin"


class User(Base):
	__tablename__ = 'users'

	id = Column(Integer, primary_key=True)
	name = Column(String(255), nullable=False)
	dob = Column(String(55))
	email = Column(String(255), nullable=False)
	passwordhash = Column(String(255), nullable=False) 
	phone = Column(String(255), nullable=False)
	about = Column(String(10000))
	gender = Column(Enum(GenderEnum))
	permissions = Column(Enum(PermissionsEnum), nullable=False)

	# these shoud go into volunteer later
	vhours = Column(Integer) #will be a seperate table later, could be merged into events
	neighborhood = Column(String(255)) # seperate table Neighborhoods
	interests = ... #seperate table 
	skills = … #seperate table
	education = Column(String(255)) #seperate table
	availability = Column(String(255)) #this will need some discussion
	last_activity = Column(Datetime(), nullable=False)
	events … #seperate table

	def __init__(self, id, name, dob=None, 
		email, passwordhash, phone, about=None, vhours=None, 
		neighborhood=None, interests=None, skills=None, gender=None, permissions, education=None, availability=None, last_activity=None):
		self.id = id
		self.name = name
		self.dob = dob
		self.email = email
		self.passwordhash = passwordhash
		self.phone = phone
		self.about = about
		self.vhours = vhours
		self.neighborhood = neighborhood
		self.interests = interests
		self.skills = skills
		self.gender = gender
		self.education = education
		self.availability = availability
		self.events = events

	def __repr__(self):
		return "User(%s, %s)" % (self.id, self.name)

	#updating in sqlalchemy works like this
	def editName(self, name):
		session = Session()
		try:
			self.name = name
			session.commit()
		except:
			session.rollback()
			raise #exception of some sort
		finally:
			session.close()

	
	def editDOB(self, dob):
		"UPDATE "
		"SET dob = dob"

	def editGender(self, gender):
		"UPDATE "
		"SET dob = dob"

	def editBio(self, bio):
		"UPDATE "
		"SET dob = dob"

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