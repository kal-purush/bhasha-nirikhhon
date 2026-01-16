from src.ctrl.AirportController import AirportController
from src.views.MainView import MainView


class MainController():
	def __init__(self):
		self.view = MainView()
		self.ctrl = AirportController()

	def main(self):
		self.view.main_menu()
		answer = self.view.get_input()

		while answer != 'q':
			if answer == '1':
				self.ctrl.add_flight()
			elif answer == '2':
				self.ctrl.search_flight()
			elif answer == '3':
				self.ctrl.sort_flights()
			elif answer == '4':
				self.ctrl.remove_all_flights_to_destination()
			elif answer == '5':
				self.ctrl.low_cost_for_destination()
			elif answer == '6':
				self.ctrl.display_destinations()
			elif answer == '7':
				self.ctrl.display_airplane_companies()
			elif answer == '8':
				self.ctrl.display_flights()
			elif answer == 'menu':
				self.view.main_menu()
			else:
				self.view.invalid_command(answer)

			answer = self.view.get_input()