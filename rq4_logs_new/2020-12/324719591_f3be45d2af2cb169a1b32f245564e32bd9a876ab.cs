using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HaloBiz.MyServices;
using Microsoft.AspNetCore.Mvc;

namespace HaloBiz.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class StateController : ControllerBase
    {
        private readonly IStatesService stateService;
        public StateController(IStatesService stateService)
        {
            this.stateService = stateService;
        }

        [HttpGet("")]
        public async Task<ActionResult> GetStates()
        {
            var response = await stateService.GetAllStates();
            return StatusCode(response.StatusCode, response);
        }

        [HttpGet("name/{name}")]
        public async Task<ActionResult> GetStateByName(string name)
        {
            var response = await stateService.GetStateByName(name);
            return StatusCode(response.StatusCode, response);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult> GetStateById(int id)
        {
            var response = await stateService.GetStateById(id);
            return StatusCode(response.StatusCode, response);
        }
        
    }
}