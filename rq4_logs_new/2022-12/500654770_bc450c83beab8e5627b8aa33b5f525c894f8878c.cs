using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Transenvios.Shipping.Api.Adapters.UserService.UserPage;
using Transenvios.Shipping.Api.Domains.ClientService.ClientPage;
using Transenvios.Shipping.Api.Domains.UserService.UserPage;
using Transenvios.Shipping.Api.Infraestructure;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Transenvios.Shipping.Api.Adapters.ClientsPage
{
    [Route("api/[controller]")]
    [ApiController]
    public class ClientsController : ControllerBase
    {
        private readonly ClientProcessor _clientProcessor;


        public ClientsController(
           ClientProcessor clientProcessor
            )
        {
            _clientProcessor = clientProcessor ?? throw new ArgumentNullException(nameof(clientProcessor));
        }

        // GET: api/<ClientsController>
        [HttpGet]
        public async Task<ActionResult<IList<Client>>> GetAllAsync()
        {
            var client = await _clientProcessor.GetAllAsync();
            return Ok(client);
        }

        // GET api/<ClientsController>/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            return "value";
        }

        // POST api/<ClientsController>
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        // PUT api/<ClientsController>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/<ClientsController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}