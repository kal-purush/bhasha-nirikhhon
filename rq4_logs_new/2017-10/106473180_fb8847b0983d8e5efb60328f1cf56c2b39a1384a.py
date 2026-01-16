import logging
import Athlete.Encyclopedia.setAttributeScores
import Athlete.Encyclopedia.Person

class Athlete(Person):
	'''A class for encapsulating an athlete.'''
	
	def __init__(self):

		self._logger = logging.getLogger(__name__)

		self._lookup = {}

		self._logger.info("Instantiated Athlete.Encyclopedia.Athlete")

	def setTeamName(team_name):
		self._team_name = team_name

	def getTeamName():
		return self._team_name

	def setAttributeScores(attribute_scores):
		self._attribute_scores = attribute_scores

	def getAttributeScores():
		return self._attribute_scores

	def setPerformanceMeasures(performance_measures):
		self._performance_measures = performance_measures

	def getPerformanceMeasures():
		return self._performance_measures

