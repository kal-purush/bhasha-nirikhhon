"""
3. Создать текстовый файл (не программно), построчно записать фамилии
сотрудников и величину их окладов. Определить, кто из сотрудников имеет оклад
менее 20 тыс., вывести фамилии этих сотрудников.
Выполнить подсчет средней величины дохода сотрудников.
"""

employees, employees_list, salary_all = {}, None, []
with open('salary.txt', 'r') as file:
    employees_list = file.readlines()

for item in employees_list:
    employees[item.split(' - ')[0]] = int(item.split(' - ')[1])

for employee in employees.keys():
    salary_all.append(employees[employee])
    if employees[employee] < 20000:
        print(f'Зарплата сотрудника {employee} менее 20000 у.е. ({employees[employee]}).')
print(f'Средняя зарплата составляет {int(sum(salary_all) / len(salary_all))} у.е.')