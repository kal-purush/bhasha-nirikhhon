"""
Configuration of syllabus server.
Edit to fit development or deployment environment.

"""
import random 

### My local development environment
#SERVER_NAME = "localhost:5000"
PORT=5000
DEBUG = True

### On ix.cs.uoregon.edu
#SERVER_NAME = "ix.cs.uoregon.edu:{}".format(random.randint(5000,8000))
PORT="{}".format(random.randint(5000,8000))
DEBUG = False # Because it's unsafe to run outside localhost