using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;
using HaloBiz.DTOs.TransferDTOs;
using HaloBiz.MyServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HaloBiz.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class StrategicBusinessUnitController : ControllerBase
    {
        private readonly IStrategicBusinessUnitService _strategicBusinessUnitService;

        public StrategicBusinessUnitController(IStrategicBusinessUnitService strategicBusinessUnitService)
        {
            this._strategicBusinessUnitService = strategicBusinessUnitService;
        }

        [HttpGet("")]
        public async Task<ActionResult> GetStrategicBusinessUnits()
        {
            var response = await _strategicBusinessUnitService.GetAllStrategicBusinessUnit();
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var strategicBusinessUnit = ((ApiOkResponse)response).Result;
            return Ok((IEnumerable<StrategicBusinessUnitTransferDTO>)strategicBusinessUnit);
        }
        [HttpGet("name/{name}")]
        public async Task<ActionResult> GetByName(string name)
        {
            var response = await _strategicBusinessUnitService.GetStrategicBusinessUnitByName(name);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var strategicBusinessUnit = ((ApiOkResponse)response).Result;
            return Ok((StrategicBusinessUnitTransferDTO)strategicBusinessUnit);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult> GetById(long id)
        {
            var response = await _strategicBusinessUnitService.GetStrategicBusinessUnitById(id);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var strategicBusinessUnit = ((ApiOkResponse)response).Result;
            return Ok((StrategicBusinessUnitTransferDTO)strategicBusinessUnit);
        }

        [HttpPost("")]
        public async Task<ActionResult> AddNew(StrategicBusinessUnitReceivingDTO strategicBusinessUnitReceiving)
        {
            var response = await _strategicBusinessUnitService.AddStrategicBusinessUnit(strategicBusinessUnitReceiving);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var strategicBusinessUnit = ((ApiOkResponse)response).Result;
            return Ok((StrategicBusinessUnitTransferDTO)strategicBusinessUnit);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateById(long id, StrategicBusinessUnitReceivingDTO strategicBusinessUnitReceiving)
        {
            var response = await _strategicBusinessUnitService.UpdateStrategicBusinessUnit(id, strategicBusinessUnitReceiving);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var strategicBusinessUnit = ((ApiOkResponse)response).Result;
            return Ok((StrategicBusinessUnitTransferDTO)strategicBusinessUnit);
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteById(int id)
        {
            var response = await _strategicBusinessUnitService.DeleteStrategicBusinessUnit(id);
            return StatusCode(response.StatusCode);
        }
    }
}