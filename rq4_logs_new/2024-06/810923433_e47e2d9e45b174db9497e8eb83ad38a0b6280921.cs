using GlobalIntegrationApi.Services;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace GlobalIntegrationApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StatusesController : ControllerBase
    {
        private readonly IGlobalIntegrationServices _globalIntegrationServices;

        public StatusesController(IGlobalIntegrationServices globalIntegrationServices)
        {
            _globalIntegrationServices = globalIntegrationServices;
        }

        [HttpGet("GetStatuses")]
        public async Task<IActionResult> GetStatuses()
        {
            var statuses = await _globalIntegrationServices.GetStatusesAsync();
            return Ok(statuses);
        }
    }
}