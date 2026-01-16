using EasyAgenda.Data.Contracts;
using Microsoft.AspNetCore.Mvc;

namespace EasyAgenda.Controllers
{
    [ApiController]
    [Route("api/")]
    public class AddressController: ControllerBase
    {
        private readonly IAddressDAL _addressRepository;

        public AddressController(IAddressDAL addressRepository)
        {
            _addressRepository= addressRepository;
        }

        [Route("v1/addresses/{cep}"), HttpGet]
        public async Task<IActionResult> GetAddress(string cep)
        {
            try
            {
                var address = await _addressRepository.GetAddress(cep);
                return Ok(address);
            }
            catch (HttpRequestException error)
            {
                return BadRequest(error.Message);
            }
            catch (Exception error) 
            {
                return StatusCode(StatusCodes.Status500InternalServerError, error.Message);
            }
        }
    }
}