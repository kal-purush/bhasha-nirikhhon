import sys

class Risk_Class:
    def __init__(self, CO2_emissions):
        #in tons
        self.CO2_emissions = CO2_emissions
        #in procents
        self.probability_influence_values = [0.05, 0.5, 1.0, 5.0, 20.0]
        self.probability_incedent_values  = [0.5, 1.0, 10.0, 20.0, 50.0]
        self.risk_low_limit = 0.01
        self.risk_high_limit = 0.15

    def calculate_matrix_risks(self):
        self.matrix_risks = [[round(self.probability_influence_values[column] * self.probability_incedent_values[row] * self.CO2_emissions * 0.01 * 0.01, 2) for column in range(len(self.probability_influence_values))] for row in range(len(self.probability_incedent_values))]

    def print_matrix_risks(self):
        for row in range(len(self.probability_incedent_values)):
            print('Probability : ',self.probability_incedent_values[row], end=" ")
            for col in range(len(self.probability_influence_values)):
                print(self.matrix_risks[row][col], end=" ")
            print("")


risk = Risk_Class(23259)
risk.calculate_matrix_risks()
risk.print_matrix_risks()