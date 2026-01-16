"""
CP1404 Week 11 Workshop - GUI program to convert miles to kilometres
Modified by Mohammad Ramiz Karim
19/11/2024
"""

from kivy.app import App
from kivy.lang import Builder

MILES_TO_KM = 1.60934


class ConvertMilesKm(App):
    """ConvertMilesKm is a Kivy App for converting miles to kilometres"""
    def build(self):
        """Build the Kivy app using the kv file"""
        self.title = "Miles to Kilometres Converter"
        self.root = Builder.load_file('convert_miles_km.kv')
        return self.root

    def calculate_km(self):
        """Calculate the kilometres from miles and update the label"""
        miles = self.validate_input()
        km = miles * MILES_TO_KM
        self.root.ids.result_label.text = f"{km:.2f}"

    def adjust_miles(self, adjustment):
        """Adjust the miles input based on the button press and update calculation"""
        miles = self.validate_input() + adjustment
        self.root.ids.miles_input.text = str(miles)
        self.calculate_km()

    def validate_input(self):
        """Validate the input to ensure it is a valid number; return 0 if invalid"""
        try:
            return float(self.root.ids.miles_input.text)
        except ValueError:
            return 0


ConvertMilesKm().run()