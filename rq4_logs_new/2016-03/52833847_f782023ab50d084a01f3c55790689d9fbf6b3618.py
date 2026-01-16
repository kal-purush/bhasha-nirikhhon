import volunteer
import admin
import orgmember
from flask import request, render_template
from flask.ext.api import status
from db import Base, Session
from app import app


@app.route('/')
def index():
	content = {'test content':'disregard'}
	return content, status.HTTP_404_NOT_FOUND

@app.route('/user', methods=['GET', 'POST', 'PUT', 'DELETE'])
def user():
	content = {'content':'blah'}
	success = {'status':'account created, yay!'}
	error = {'error': 'Error in JSON/SQL syntax'}
	if request.method == 'POST':
		data = request.json
		if data['permissions'] == 'volunteer':
			if volunteer.Volunteer.createVolunteer(data):
				return success, status.HTTP_200_OK
		if data['permissions'] == 'admin':
			if admin.Admin.createAdmin(data):
				return success, status.HTTP_200_OK
		if data['permissions'] == 'orgmember':
			if orgmember.OrgMember.createMember(data):
				return success, status.HTTP_200_OK
		else:
			return error, HTTP_500_INTERNAL_SERVER_ERROR

	if request.method == 'GET':
		return content, status.HTTP_200_OK