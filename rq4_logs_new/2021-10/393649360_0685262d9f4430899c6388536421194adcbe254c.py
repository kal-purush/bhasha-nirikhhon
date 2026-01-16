from tabulate import tabulate


emp_list = [{'name': 'John Doe',
             'department': 'Cleaning',
             'post': 'Big cleaner',
             'salary': '1000',
             'emp_id': '1337'}, ]


class Department:
    def __init__(self, emp_name='John Doe', dep_name='Cleaning',
                 emp_pos='Big Cleaner', salary='1000', emp_id='1337',
                 ):
        self.dep_name = dep_name
        self.emp_name = emp_name
        self.post = emp_pos
        self.salary = salary
        self.id = emp_id

    @staticmethod
    def show_employees():
        return print(tabulate(emp_list))

    def get_department(self):
        return self.dep_name

    @staticmethod
    def check_employee(emp_id):
        for i in emp_list:
            if i.get('id') == emp_id:
                return True
            else:
                return False


class Employee(Department):

    def add_employee(self):
        emp_id = input("Enter Employ Id : ")
        if Department.check_employee(emp_id) is True:
            print("Employee already exists\nTry Again\n")
        else:
            name = input("Enter Employ Name : ")
            post = input("Enter Employ Post : ")
            salary = input("Enter Employ Salary : ")
            department = Department.get_department(self)
            employee = {'name': name, 'department': department,
                        'post': post, 'salary': salary, 'emp_id': emp_id}
            emp_list.append(employee)

    @staticmethod
    def find_employee():
        emp_idd = input("Enter Employ Id : ")
        for i in emp_list:
            if i.get('id') == emp_idd:
                return print(i)
            else:
                print('No matches found')

    @staticmethod
    def remove_employee():
        emp_id = input("Enter Employ Id : ")
        for i in emp_list:
            if i.get('id') == emp_id:
                emp_list.remove(i)
                print('Removed')
            else:
                print('No employees found')

    @staticmethod
    def list_employees():
        Department.show_employees()


def menu():

    employee = Employee()
    print("== Welcome to Employee Management Record ==")
    print("Make your choice: ")
    print("1 to Add Employee\n2 to Remove Employee\n3 to Find Employee\n4 to Display Employees")
    print("5 to Exit")

    ch = input("Enter your Choice ")
    c = {'1': Employee.add_employee,
         '2': Employee.remove_employee,
         '3': Employee.find_employee,
         '4': Employee.list_employees,
         '5': exit}
    method = c.get(ch, exit)
    method(employee)


if __name__ == '__main__':
    menu()



