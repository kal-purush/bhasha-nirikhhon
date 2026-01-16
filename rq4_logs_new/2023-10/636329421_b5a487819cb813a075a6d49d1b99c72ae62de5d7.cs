using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using PCLine_computer_shops.Dtos;
using PCLine_computer_shops.InterfaceReposiotry;
using PCLine_computer_shops.Models;

namespace PCLine_computer_shops.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EmployeeController : ControllerBase
    {
        private readonly IEmployeeRepository _employeeRepository;
        private readonly IMapper _mapper;

        public EmployeeController(IEmployeeRepository employeeRepository, IMapper mapper)
        {
            _employeeRepository = employeeRepository;
            _mapper = mapper;
        }

        [HttpGet("Get")]
        public async Task<IActionResult> GetAllEmployees([FromQuery] string searchString = "")
        {
            var employees = await _employeeRepository.GetAllEmployees(searchString);

            if (employees == null)
            {
                return NotFound();
            }

            var employeesGet = _mapper.Map<List<EmployeeGetDto>>(employees);

            return Ok(employeesGet);
        }

        [HttpGet("Get/{shopId}")]
        public async Task<IActionResult> GetAllEmployeeForShp(int shopId)
        {
            var shop = await _employeeRepository.GetAllEmployeesForShopById(shopId);

                if(shop == null)
                {
                    return NotFound();
                }
            
            var employesGet = _mapper.Map<List<EmployeeGetDto>>(shop);

            return Ok(employesGet);
        }

        [HttpGet("Get/{shopId}/GetEmployee/{employeeId}")]
        public async Task<IActionResult> GetEmployeeById(int shopId, int employeeId)
        {
            var employee = await _employeeRepository.GetEmployeeById(shopId, employeeId);

            if (employee == null)
            {
                return NotFound();
            }

            var employeeGet = _mapper.Map<EmployeeGetDto>(employee);

            return Ok(employeeGet);
        }

        [HttpPost("Post/{shopId}")]
        public async Task<IActionResult> CreateEmployee(int shopId, [FromBody] EmployeeCreateDto newEmployee)
        {
            var employeeCreate = _mapper.Map<Employee>(newEmployee);

            await _employeeRepository.CreateEmployeeForShop(shopId, employeeCreate);

            if (employeeCreate == null)
            {
                return NotFound();
            }

            var employeeGet = _mapper.Map<EmployeeGetDto>(employeeCreate);

            return CreatedAtAction(nameof(GetEmployeeById), new { shopId = shopId, employeeId = employeeGet.EmployeeId }, employeeGet);
        }

        [HttpPut("Put/{shopId}/{employeeId}")]
        public async Task<IActionResult> UpdateEmployee(int shopId, int employeeId, [FromBody] EmployeeCreateDto employeeUpdate)
        {
            var toUpdateEmployee = _mapper.Map<Employee>(employeeUpdate);

            toUpdateEmployee.EmployeeId = employeeId;

            toUpdateEmployee.ShopId = shopId;

            await _employeeRepository.UpdateEmployee(shopId, toUpdateEmployee);

            return NoContent();
        }

        [HttpDelete("Delete/{shopId}/employee/{employeeId}")]
        public async Task<IActionResult> DeleteEmployee(int shopId, int employeeId)
        {
            var deleteEmployee = await _employeeRepository.DeleteEmployee(shopId, employeeId);

            if (deleteEmployee == null)
            {
                return NotFound();
            }

            return NoContent();
        }
    }
}