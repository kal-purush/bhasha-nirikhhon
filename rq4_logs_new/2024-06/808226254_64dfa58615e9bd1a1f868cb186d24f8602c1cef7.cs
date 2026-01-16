using ActivityFinder.Server.Models;
using Microsoft.AspNetCore.Mvc;

namespace ActivityFinder.Server.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class AddressController : Controller
    {
        private readonly ILogger<AddressController> _logger;

        public AddressController(ILogger<AddressController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        [Route("{input}")]
        public IActionResult GetBySearchInput(string input)
        {
            var result = new AddressSearch().GetAddressByName(input);

            if (!result.Success)
                return NotFound(result.Message);

            return Ok(result.Value!.ToAddressDto());
        }
    }
}