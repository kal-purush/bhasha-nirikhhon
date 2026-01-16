"""
Elections page controller.
"""

__author__ = 'Roshni Kaushik <rsk8@rice.edu>'

import json
import logging
import pages
import webapp2
from  mail import sendConfirmation

from authentication import auth
from models.user import User

import models.election
import models.position

PAGE_URI = '/elections'
#MY_PAGE_URI = '/my'

class ElectionsHandler(webapp2.RequestHandler):
		def get(self):
			user = auth.require_login(self)
			ongoingElections = models.election.get_ongoing_elections()
			logging.info("ongoing elections: %s", ongoingElections)
			positions = {}
			stripped_election_names = {}
			for election in ongoingElections:
				positions[election['title']] = models.position.get_positions_for_election(election['title'])
				stripped_election_names[election['title']] = election['title'].replace(' ', '')
			logging.info("positions: %s", positions)
			view = pages.render_view(PAGE_URI, {'ongoingElections': ongoingElections, 'positions': positions, 'stripped_election_names': stripped_election_names})
			pages.render_page(self, view)

		def post(self):
			# Authenticate user
			user = auth.get_logged_in_user()
			if not user:
				return      # Should return error message here
					
			# Create election
			data = json.loads(self.request.get('data'))
			logging.info('Election Post: %s', data)
			election = models.election.create_election(user, data)

			#Respond
			if not election:
				data['id'] = 'Duplicate Election'
			else:
				data['id'] = str(election.key())
			self.response.out.write(json.dumps(data))

class PositionsHandler(webapp2.RequestHandler):
		def post(self):
			# Authenticate user
			user = auth.get_logged_in_user()
			if not user:
				return      # Should return error message here
					
			# Create position
			data = json.loads(self.request.get('data'))
			logging.info('Position Post: %s', data)
			position = models.position.create_position(user, data)
			#Respond
			if not position:
				data['id'] = 'Duplicate Position'
			else:
				data['id'] = str(position.key())
			self.response.out.write(json.dumps(data))			
			
class GarbageHandler(webapp2.RequestHandler):
		def post(self):
			# Authenticate user
			user = auth.get_logged_in_user()
			if not user:
				return      # Should return error message here
			election_id = self.request.get('id')
			election = models.election.get_election(election_id)

			# Make sure the user is not trying to delete someone else's election
			assert election.user.key() == user.key()

			# Gets all associated positions
			positions_json = models.position.get_positions_for_election(election.get_title())
			if len(positions_json) > 0:
				self.response.out.write('Please delete all asosociated positions before deleting the election')
			else:
				models.election.delete_election(election)
				self.response.out.write('Success!')

class PositionsGarbageHandler(webapp2.RequestHandler):
		def post(self):
			# Authenticate user
			user = auth.get_logged_in_user()
			if not user:
				return      # Should return error message here
			position_id = self.request.get('id')
			position = models.position.get_position(position_id)
			
			# Make sure the user is not trying to delete someone else's position
			assert position.user.key() == user.key()

			models.position.delete_position(position)

			self.response.out.write('Success!')