using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GreenMonkeysMVC.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Mvc.Rendering;
using GreenMonkeysMVC.Models.ViewModels;

namespace GreenMonkeysMVC.Controllers
{
    public class DepartmentsController : Controller
    {
        private readonly IConfiguration _config;
        public DepartmentsController(IConfiguration config)
        {
            _config = config;
        }
        public SqlConnection Connection
        {
            get
            {
                return new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            }
        }
        // GET: Departments
        public ActionResult Index()
        {
            var getEmployees = GetEmployeeCount();

            using (SqlConnection conn = Connection)
            {
                conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT d.[Name] AS DepartmentName, COUNT(e.Id) AS EmployeeCount, d.Budget, d.Id 
                                        FROM Department d LEFT JOIN Employee e ON d.Id = e.DepartmentId
                                        GROUP BY d.[Name], d.Budget, d. Id";
                    var reader = cmd.ExecuteReader();

                    var departments = new List<Department>();

                    while (reader.Read())
                    {
                        departments.Add(new Department
                        {
                            Id = reader.GetInt32(reader.GetOrdinal("Id")),
                            Name = reader.GetString(reader.GetOrdinal("DepartmentName")),
                            Budget = reader.GetInt32(reader.GetOrdinal("Budget"))
                        });
                        reader.Close();

                        var viewModel = new DepartmentViewModel
                        {
                            Departments = departments,
                            Employees = getEmployees
                        };
                        reader.Close();
                        return View(viewModel);
                    }
                    reader.Close();
                    return NotFound();
                }
            }
        }

        // GET: Departments/Details/5
        public ActionResult Details(int id)
        {
            return View();
        }

        // GET: Departments/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Departments/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(IFormCollection collection)
        {
            try
            {
                // TODO: Add insert logic here

                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: Departments/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: Departments/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit(int id, IFormCollection collection)
        {
            try
            {
                // TODO: Add update logic here

                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: Departments/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: Departments/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(int id, IFormCollection collection)
        {
            try
            {
                // TODO: Add delete logic here

                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }
    
    // GET: Private method to get a list of Employees

    private List<Employee> GetEmployeeCount()
    {
        using (SqlConnection conn = Connection)
        {
            conn.Open();
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"SELECT Id, DepartmentId
                                       FROM Employee";

                    var reader = cmd.ExecuteReader();

                    var exercises = new List<Employee>();

                    while (reader.Read())
                    {
                        exercises.Add(new Employee
                        {
                            Id = reader.GetInt32(reader.GetOrdinal("Id")),
                            DepartmentId = reader.GetInt32(reader.GetOrdinal("DepartmentId")),
                        });
                    }
                    reader.Close();
                    return exercises;

                }
            }
        }
    }
}