using church_mgt_core.services.interfaces;
using church_mgt_dtos.DepartmentDtos;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace church_mgt_api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DepartmentsController : ControllerBase
    {
        private readonly IDepartmentService _departmentService;
        public DepartmentsController(IDepartmentService departmentService)
        {
            _departmentService = departmentService;
        }

        [HttpGet]
        public IActionResult Departments()
        {
            var result = _departmentService.GetAllDepartments();

            return StatusCode(result.StatusCode, result);
        }

        [HttpGet("{departmentId}")]
        public async Task<IActionResult> GetDepartment(string departmentId)
        {
            var result = await _departmentService.GetDepartmentById(departmentId);

            return StatusCode(result.StatusCode, result);
        }

        [HttpGet("byuserid/{userId}")]
        public IActionResult GetDepartmentsByUserId(string userId)
        {
            var result = _departmentService.DepartmentsByUserId(userId);

            return StatusCode(result.StatusCode, result);
        }

        [HttpGet("membersin/{departmentId}")]
        public async Task<IActionResult> GetMembersInDepartments(string departmentId)
        {
            var result =await _departmentService.GetMembersInDepartmentAsync(departmentId);
            return StatusCode(result.StatusCode, result);
        }

        [HttpPost]
        public async Task<IActionResult> AddDepartment(AddDepartmentDto model)
        {
            var result = await _departmentService.CreateDepartmentAsync(model);
            return StatusCode(result.StatusCode, result);
        }

        [HttpPut("{departmentId}")]
        public async Task<IActionResult> UpdateDepartment(string departmentId, [FromBody]AddDepartmentDto model)
        {
            var result = await _departmentService.UpdateDepartment(departmentId, model);
            return StatusCode(result.StatusCode, result);
        }

        [HttpDelete("{departmentId}")]
        public async Task<IActionResult> DeleteDepartment(string departmentId)
        {
            var result = await _departmentService.DeleteDepartmentByIdAsync(departmentId);
            return StatusCode(result.StatusCode, result);
        }

        [HttpPost("{userId}/addto-department")]
        public async Task<IActionResult> AddMemberToDepartment(string userId, [FromForm]string departmentId)
        {
            var result = await _departmentService.AddMemberToDepartment(userId, departmentId);
            return StatusCode(result.StatusCode, result);
        }
    }
}