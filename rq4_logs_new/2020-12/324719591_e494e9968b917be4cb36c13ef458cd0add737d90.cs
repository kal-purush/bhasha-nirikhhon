using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;
using HaloBiz.MyServices;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace HaloBiz.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class ServicesController : ControllerBase
    {
        private readonly IServicesService _servicesService;

        public ServicesController(IServicesService servicesService)
        {
            this._servicesService = servicesService;
        }

        [HttpGet("")]
        public async Task<ActionResult> GetService()
        {
            var response = await _servicesService.GetAllServices();
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var services = ((ApiOkResponse)response).Result;
            return Ok(services);
        }

        [HttpGet("name/{name}")]
        public async Task<ActionResult> GetByName(string name)
        {
            var response = await _servicesService.GetServiceByName(name);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var service = ((ApiOkResponse)response).Result;
            return Ok(service);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult> GetById(long id)
        {
            var response = await _servicesService.GetServiceById(id);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var service = ((ApiOkResponse)response).Result;
            return Ok(service);
        }

        [HttpPost("")]
        public async Task<ActionResult> AddNew(ServicesReceivingDTO servicesReceivingDTO)
        {
            var response = await _servicesService.AddService(servicesReceivingDTO);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var service = ((ApiOkResponse)response).Result;
            return Ok(service);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateById(long id, ServicesReceivingDTO servicesReceivingDTO)
        {
            var response = await _servicesService.UpdateServices(id, servicesReceivingDTO);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceGroup = ((ApiOkResponse)response).Result;
            return Ok(serviceGroup);
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteById(int id)
        {
            var response = await _servicesService.DeleteServiceGroup(id);
            return StatusCode(response.StatusCode);
        }
        
    }
}