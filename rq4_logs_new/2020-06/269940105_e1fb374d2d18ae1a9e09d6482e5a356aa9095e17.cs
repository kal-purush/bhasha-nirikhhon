using BLL.Services.Interfaces;
using OrganisationArchive.DAL.Models;
using OrganisationArchive.DAL.Repository.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace BLL.Services.Implementations
{
    public class EmployeeService : IEmployeeService
    {
        private IUOW _uow;

        public EmployeeService(IUOW uOW)
        {
            _uow = uOW;
        }
        public void AddEmployee(Employee employee)
        {
            _uow.Employee.Create(employee);
            _uow.commit();
        }

        public void DeleteEmployee(Employee employee)
        {
            _uow.Employee.Delete(employee);
            _uow.commit();
        }

        public IEnumerable<Employee> getEmployee()
        {
           return _uow.Employee.FindAll();
        }

        public Employee GetEmployeenById(int Id)
        {
            return _uow.Employee.Get(Id);
        }

        public void UpdateEmployee(Employee employee)
        {
            _uow.Employee.Update(employee);
            _uow.commit();
        }
    }
}