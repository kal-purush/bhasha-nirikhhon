using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WebStore.Infrastructure.Services.Interfaces;
using WebStore.Models;

namespace WebStore.Infrastructure.Services
{
    public class InMemoryEmployeesData : IEmployeesData
    {
        private readonly List<Employee> _Employees;
        private int _CurrentMaxId;
        public InMemoryEmployeesData()
        {
            _Employees = EmployeeContext.Employees;
            _CurrentMaxId = _Employees.DefaultIfEmpty().Max(x => x?.Id ?? 1);
        }
        public IEnumerable<Employee> Get()
        {
            return _Employees;
        }

        public Employee Get(int id)
        {
            return _Employees.FirstOrDefault(x => x.Id == id);
        }

        public int Add(Employee employee)
        {
            if(employee == null) 
                throw new ArgumentNullException();

            if (_Employees.Contains(employee)) 
                return employee.Id;

            employee.Id = ++_CurrentMaxId;
            _Employees.Add(employee);

            return employee.Id;
        }

        public void Update(Employee employee)
        {
            if (employee == null)
                throw new ArgumentNullException();

            if (_Employees.Contains(employee))
                return;

            var dbEmployee = Get(employee.Id);
            if(dbEmployee == null)
                return;

            dbEmployee.FirstName = employee.FirstName;
            dbEmployee.LastName = employee.LastName;
            dbEmployee.Patronymic = employee.Patronymic;
            dbEmployee.BirthDate = employee.BirthDate;
            dbEmployee.CityDepartment = employee.CityDepartment;
            dbEmployee.Salary = employee.Salary;

        }

        public bool Delete(int id)
        {
            var dbEmployee = Get(id);

            return dbEmployee != null && _Employees.Remove(dbEmployee);
        }
    }
}