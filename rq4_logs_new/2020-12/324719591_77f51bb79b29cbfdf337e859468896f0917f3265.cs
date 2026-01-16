using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;
using HaloBiz.MyServices;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;


namespace HaloBiz.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class ServiceGroupController : ControllerBase
    {
        private readonly IServiceGroupService _serviceGroupService;

        public ServiceGroupController(IServiceGroupService serviceGroupService)
        {
            this._serviceGroupService = serviceGroupService;
        }

        [HttpGet("")]
        public async Task<ActionResult> GetServiceGroup()
        {
            var response = await _serviceGroupService.GetAllServiceGroups();
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceGroups = ((ApiOkResponse)response).Result;
            return Ok(serviceGroups);
        }

        [HttpGet("name/{name}")]
        public async Task<ActionResult> GetByName(string name)
        {
            var response = await _serviceGroupService.GetServiceGroupByName(name);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceGroup = ((ApiOkResponse)response).Result;
            return Ok(serviceGroup);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult> GetById(long id)
        {
            var response = await _serviceGroupService.GetServiceGroupById(id);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceGroup = ((ApiOkResponse)response).Result;
            return Ok(serviceGroup);
        }

        [HttpPost("")]
        public async Task<ActionResult> AddNew(ServiceGroupReceivingDTO serviceGroupReceivingDTO)
        {
            var response = await _serviceGroupService.AddServiceGroup(serviceGroupReceivingDTO);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var operatingEntity = ((ApiOkResponse)response).Result;
            return Ok(operatingEntity);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateById(long id, ServiceGroupReceivingDTO serviceGroupReceivingDTO)
        {
            var response = await _serviceGroupService.UpdateServiceGroup(id, serviceGroupReceivingDTO);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var serviceGroup = ((ApiOkResponse)response).Result;
            return Ok(serviceGroup);
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteById(int id)
        {
            var response = await _serviceGroupService.DeleteServiceGroup(id);
            return StatusCode(response.StatusCode);
        }
    }
}