using Microsoft.AspNetCore.Mvc;
using SimpleRestApi.Entities;
using SimpleRestApi.Service;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace SimpleRestApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CustomerController : ControllerBase
    {
        public ICustomerService _customerService;

        public CustomerController(ICustomerService customerService)
        {
            _customerService = customerService;
        }


        // GET: api/<CustomerController>
        [HttpGet]
        public ActionResult<List<Customer>> GetAll()
        {
            return _customerService.GetAllCustomers();
        }

        // POST api/<CustomerController>
        [HttpPost]
        public void Post([FromBody] Customer customer)
        {
            _customerService.CreateNewCustomer(customer);
        }

        // DELETE api/<CustomerController>/5
        [HttpDelete("{id}")]
        public ActionResult Delete([FromRoute] int id)
        {
            var customer = _customerService.GetCustomerById(id);

            if (customer == null)
            {
                return NotFound();
            }

            _customerService?.DeleteCustomer(id);
            return NoContent();
        }

        // GET: api/customer/{id}
        [HttpGet("{id}")]
        public ActionResult<Customer> GetCustomerById(int id)
        {
            var customer = _customerService.GetCustomerById(id);

            if (customer == null)
            {
                return NotFound();
            }

            return customer;
        }
    }
}