from src.repo.db_repo import DbRepository
from src.validator.Validator import Validator
from src.views.TripView import TripView


class AirportController():
	def __init__(self):
		self.db = DbRepository()
		self.view = TripView()


	def add_flight(self):
		result = self.view.add_new_trip()

		if Validator.not_none(result):
			self._add_flight(result)


	def search_flight(self):
		result = self.view.search_flight()

		if Validator.not_none(result):
			flights = self.db.find_flight(result)
			self.view.display_list(flights, 'The flights are: ')


	def sort_flights(self):
		self.view.show_flights_ascending_price()
		result = self.db.sort_flights()
		self.view.display_list(result, 'The flights are: ')


	def average_price_per_company(self):
		self.view.show_average_per_airplane_company()
		result = self.db.average_company()
		self.view.display_list(result, 'The companies are: ')


	def _add_flight(self, results):
		company = self.db.get_company_by_name(results[1:])
		price = results[1:]
		destination = self.db.get_destination_by_name(results[1:])

		if Validator.not_none(company, True) and Validator.not_none(destination, True):
			self.db.add_flight(company, destination, price)





