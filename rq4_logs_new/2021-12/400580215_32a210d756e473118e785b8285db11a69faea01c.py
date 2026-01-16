"""
Name: Caleb Quattlebaum
sales_force.py

Purpose: Create a class that uses data from an external file and creates objects
         using the imported SalesPerson class to compare and organize employee data

Certification of Authenticity:
I certify that this assignment is entirely my own work.
"""

from sales_person import SalesPerson

class SalesForce:
    """
    Creates an object that is a list of objects made from data of an
    external file and created using the SalesPerson class from
    sales_person.py. With methods to add data from an external
    file, get a report of all employees met quota status, and to
    find the top seller and individual sales for all employees.
    """

    def __init__(self):
        self.sales_people = []

    def add_data(self, file_name):
        for employee in open(file_name, 'r').readlines():
            employee = employee.replace('\n', '')
            people = employee.split(', ')
            employee_id = int(people[0])
            name = people[1]
            sales_str = people[2]
            sales = sales_str.split(' ')
            employee = SalesPerson(employee_id, name)
            for sale in sales:
                employee.enter_sale(float(sale))
            self.sales_people.append(employee)

    def quota_report(self, quota):
        quota_report = []
        for employee in self.sales_people:
            employee_report = [employee.get_id(), employee.get_name(),
                               employee.total_sales(), employee.met_quota(quota)]
            quota_report.append(employee_report)
        return quota_report

    def top_seller(self):
        top = []
        all_sales = []
        for employee in self.sales_people:
            all_sales.append(employee.total_sales())
        for lowest in range(len(all_sales)):
            position = lowest
            for sale in range(lowest + 1, len(all_sales)):
                if all_sales[sale] < all_sales[position]:
                    position = sale
            all_sales[lowest], all_sales[position] = all_sales[position], all_sales[lowest]
        for employee in self.sales_people:
            if employee.total_sales() == all_sales[-1]:
                top.append(employee)
        return top

    def individual_sales(self, employee_id):
        for position in range(len(self.sales_people)):
            if self.sales_people[position].get_id() == int(employee_id):
                return self.sales_people[position]