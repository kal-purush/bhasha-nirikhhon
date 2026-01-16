using BusinessLayer.Entities;
using BusinessLayer.Interfaces;
using BusinessLayer.Services;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EmployeesController : ControllerBase
    {
        private readonly IEmployeeService _service;
        private readonly IEmployeeRepository _repository;
        public EmployeesController(IEmployeeService service, IEmployeeRepository repository)
        {
            _service = service;
            _repository = repository;
        }
        // GET api/values
        [HttpGet]
        public async Task<ActionResult<IEnumerable<EmployeeDTO>>> Get()
        {
            return Ok(await _service.GetEmployees());
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public async Task<ActionResult<EmployeeDTO>> Get(int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var employee = await _service.GetById(id);

            if (employee == null)
            {
                return NotFound();
            }

            return Ok(employee);
        }
      
    }
}