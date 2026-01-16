//coded by JENN
using Dapper;
using DFBangazon.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace DFBangazon.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DepartmentController :ControllerBase
    {
        private readonly IConfiguration _config;
        private object listOfDepartments;

        public DepartmentController(IConfiguration config)
        {
            _config = config;
        }

        public IDbConnection Connection
        {
            get
            {
                return new SqlConnection(_config.GetConnectionString("DefaultConnection"));
            }
        }

        [HttpGet]
        public async Task<IActionResult> Get(string _include, string _filter, int _gt)
        {
            using (IDbConnection conn = Connection)
            {

                //If the query string parameter of? _include = employees is provided, then all employees in the department(s) should be included in the response.

                string sql = "SELECT * FROM Department";

                if (_include == "employees")
                {
                    Dictionary<int, Department> listOfDepartments = new Dictionary<int, Department>();

                    sql = @"SELECT
                            d.Id,
                            d.DeptName,
                            d.ExpenseBudget,
                            e.Id,
                            e.FirstName,
                            e.LastName,
                            e.IsSupervisor,
                            
                            e.DepartmentId
                            FROM Department d
                            JOIN Employee e ON d.Id = e.DepartmentId";


                    var departmentEmployees = await conn.QueryAsync<Department, Employee, Department>(sql,
                        (department, employee) =>
                        {
                            if (!listOfDepartments.ContainsKey(department.Id))
                            {
                                listOfDepartments[department.Id] = department;
                            }
                            listOfDepartments[department.Id].EmployeeList.Add(employee);
                            return department;
                        });
                    return Ok(listOfDepartments.Values);
                }

                    //If the query string parameters of?_filter=budget&_gt=300000 is provided on a request for the list of departments, then any department whose budget is $300, 000, or greater, should be in the response.

                if (_filter == "budget" && _gt >= 300000)
                {
                   
                    sql = $@"SELECT * FROM Department WHERE ExpenseBudget >= {_gt}";
                        
                }

                var departmentBudget = await conn.QueryAsync<Department>(sql);
                return Ok(departmentBudget);


            }

        }

            
    }
}