using Application.IServices;
using Data.Dtos;
using Data.ViewModels;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PharmacyManager.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CustomerController : Controller
    {
        private readonly ICustomerService _customerService;
        public CustomerController(ICustomerService customerService)
        {
            _customerService = customerService;
        }

        [HttpGet("AllCustomer")]
        public async Task<IActionResult> GetAllCustomer()
        {
            try
            {
                if (ModelState.IsValid)
                {
                    List<CustomerView> result = await _customerService.getAllCustomer();
                    return Ok(result);
                }
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
            return BadRequest(new { message = "Get all customer failed" });
        }

        [HttpPost("CreateOrUpdateCustomer")]
        public async Task<IActionResult> CreateOrUpdateCustomer(CustomerDto request)
        {
            try
            {
                if (ModelState.IsValid)
                {
                    StatusView result = await _customerService.updateOrCreateCustomer(request);
                    return Ok(result);
                }
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
            return BadRequest(new { message = "Create or update customer failed" });
        }

        [HttpDelete("DeleteCustomer")]
        public async Task<IActionResult> DeleteProduct(string cusId)
        {
            try
            {
                if (ModelState.IsValid)
                {
                    StatusView result = await _customerService.deleteCustomer(cusId);
                    return Ok(result);
                }
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
            return BadRequest(new { message = "Delete customer failed" });
        }
    }
}