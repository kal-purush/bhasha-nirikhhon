using Employee.API.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Employee.API.Controllers
{
    [ApiController]
    [Route("api/employees")]
    public class EmployeeController: ControllerBase
    {
        private readonly ILogger<EmployeeController> _logger;

        public EmployeeController(ILogger<EmployeeController> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        [HttpGet]
        public IActionResult GetEmployees()
        {
            try
            {
                return Ok(EmployeesDataStore.Current.Employees);
            }
            catch (Exception ex)
            {
                _logger.LogCritical($"exception while getting employee details", ex);
                return StatusCode(500, "problem occured while handling the request");
            }
        }

        [HttpGet("{id}", Name = "GetEmployees")]
        public IActionResult GetEmployee(int id)
        {
            var employeeToReturn = EmployeesDataStore.Current.Employees.FirstOrDefault(c => c.Id == id);
           
            if(employeeToReturn == null)
            {
                return NotFound();
            }

            return Ok(employeeToReturn);
        }

        [HttpPost]
        public IActionResult CreateEmployee([FromBody] EmployeeCreationDto employee)
        {

            if(employee.Description == employee.Name)
            {
                ModelState.AddModelError("Description", "The provided description should be different from the name");
            }

            if (!ModelState.IsValid)
            {
               return BadRequest(ModelState);
            }

            var maxEmployeeId = EmployeesDataStore.Current.Employees.Max(e => e.Id);

            var finalEmployee = new EmployeeDto()
            {
                Id = ++maxEmployeeId,
                Name = employee.Name,
                Description = employee.Description,
                Level = employee.Level,
                Speciality = employee.Speciality,
                Animals = employee.Animals,
                Shows= employee.Shows,
    };

            EmployeesDataStore.Current.Employees.Add(finalEmployee);

            return CreatedAtAction("GetEmployees",
                new {id = finalEmployee.Id}, finalEmployee);
        }
        [HttpPut("{id}")]
        public IActionResult UpdateEmployee(int id, [FromBody] EmployeeUpdateDto employee)
        {
            if (employee.Description == employee.Name)
            {
                ModelState.AddModelError("Description", "The provided description should be different from the name");
            }

            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var emp = EmployeesDataStore.Current.Employees.FirstOrDefault(c => c.Id == id);
            if (emp == null)
            {
                return NotFound();
            }

            emp.Name = employee.Name;
            emp.Description = employee.Description;
            emp.Level = employee.Level;
            emp.Speciality = employee.Speciality;
            emp.Animals = employee.Animals;
            emp.Shows = employee.Shows;

            return NoContent();
        }
        [HttpDelete("{id}")]
        public IActionResult DeletEmployee(int id)
        {
            var employee = EmployeesDataStore.Current.Employees.FirstOrDefault(c => c.Id == id);

            if (employee == null)
            {
                return NotFound();
            }

            EmployeesDataStore.Current.Employees.Remove(employee);

            return NoContent();
        }
    }
}