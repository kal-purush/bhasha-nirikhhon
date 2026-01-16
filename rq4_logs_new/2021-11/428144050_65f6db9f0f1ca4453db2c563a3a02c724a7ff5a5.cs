using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using EmployeeManagement.Application.Interface.IDepartment;
using EmployeeManagement.Data.Entities;

namespace EmployeeManagement.APIs.Controllers.DepartmentController
{
    [Route("api/[controller]")]
    [ApiController]
    public class DepartmentsController : ControllerBase
    {
        private readonly IDepartmentService _departmentService;

        public DepartmentsController(IDepartmentService departmentService)
        {
            _departmentService = departmentService;
        }

        // POST: api/departments
        [HttpPost]
        public async Task<IActionResult> AddDepartment([FromBody]Department department)
        {
            var result = await _departmentService.AddDepartment(department);
            return Ok(result);
        }

        // GET: api/departments
        [HttpGet]
        [Route("getTree")]
        public async Task<IActionResult> GetTreeDepartment()
        {
            var result = await _departmentService.GetTreeDepartment();
            return Ok(result);
        }

    }
}