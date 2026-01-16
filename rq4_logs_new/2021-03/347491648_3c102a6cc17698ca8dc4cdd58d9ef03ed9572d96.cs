using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WebStore.Models;

namespace WebStore.Controllers
{
    public class EmployeesController : Controller
    {

        private readonly List<Employee> _Employees;

        public EmployeesController()
        {
            _Employees = EmployeeContext.Employees;
        }
        public IActionResult Index()
        {
            return View(_Employees);
        }

        public IActionResult Details(int Id)
        {
            var employee = _Employees.FirstOrDefault(x => x.Id == Id);

            if (employee == null)
                return RedirectToAction("NotFoundPage", "Home");

            return View(employee);
        }

        public IActionResult Edit(int Id)
        {
            var employee = _Employees.FirstOrDefault(x => x.Id == Id);

            if (employee == null)
                return RedirectToAction("NotFoundPage", "Home");

            return View(employee);
        }

        [HttpPost]
        public IActionResult Edit(Employee modifiedEmployee)
        {
            var employee = _Employees.FirstOrDefault(x => x.Id == modifiedEmployee.Id);

            if (employee == null)
                return RedirectToAction("NotFoundPage", "Home");

            employee.FirstName = modifiedEmployee.FirstName;
            employee.LastName = modifiedEmployee.LastName;
            employee.Patronymic = modifiedEmployee.Patronymic;
            employee.BirthDate = modifiedEmployee.BirthDate;
            employee.CityDepartment = modifiedEmployee.CityDepartment;
            employee.Salary = modifiedEmployee.Salary;

            return RedirectToAction("Index");
        }

        public IActionResult Delete(int Id)
        {
            var employee = _Employees.FirstOrDefault(x => x.Id == Id);

            if (employee == null)
                return RedirectToAction("NotFoundPage", "Home");

            return View(employee);
        }

        [HttpPost]
        public IActionResult Delete(Employee deletingEmployee)
        {
            var employee = _Employees.FirstOrDefault(x => x.Id == deletingEmployee.Id);

            if (employee == null)
                return RedirectToAction("NotFoundPage", "Home");

            _Employees.Remove(employee);

            return RedirectToAction("Index");
        }
    }
}