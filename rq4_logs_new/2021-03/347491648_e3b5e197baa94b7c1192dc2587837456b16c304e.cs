using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using WebStore.Models;

namespace WebStore.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index() => View();

        public IActionResult SecondAction(string id) => Content($"Action with value id:{id}");

        public IActionResult Employees()
        {
            return View(EmployeeContext.Employees);
        }

        public IActionResult EmployeeDetail(int employeeId)
        {
            var employee = EmployeeContext.Employees.FirstOrDefault(x => x.Id == employeeId);
            return employee != null ? View(employee) : null;
        }
    }
}