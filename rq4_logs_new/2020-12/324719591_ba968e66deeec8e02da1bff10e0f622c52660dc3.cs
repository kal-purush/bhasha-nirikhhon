using HaloBiz.DTOs.ApiDTOs;
using HaloBiz.DTOs.ReceivingDTOs;
using HaloBiz.DTOs.TransferDTOs;
using HaloBiz.MyServices;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HaloBiz.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class DivisionController : ControllerBase
    {
        private readonly IDivisonService _divisonService;

        public DivisionController(IDivisonService divisonService)
        {
            this._divisonService = divisonService;
        }
        [HttpGet("")]
        public async Task<ActionResult> GetDivisions()
        {
            var response = await _divisonService.GetAllDivisions();
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var division = ((ApiOkResponse)response).Result;
            return Ok((IEnumerable<DivisionTransferDTO>)division);
        }

        [HttpGet("name/{name}")]
        public async Task<ActionResult> GetByName(string name)
        {
            var response = await _divisonService.GetDivisionByName(name);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var division = ((ApiOkResponse)response).Result;
            return Ok((DivisionTransferDTO)division);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult> GetById(long id)
        {
            var response = await _divisonService.GetDivisionnById(id);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var division = ((ApiOkResponse)response).Result;
            return Ok((DivisionTransferDTO)division);
        }

        [HttpPost("")]
        public async Task<ActionResult> AddNewBranch(DivisionReceivingDTO divisionReceiving)
        {
            var response = await _divisonService.AddDivision(divisionReceiving);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var division = ((ApiOkResponse)response).Result;
            return Ok((DivisionTransferDTO)division);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateById(long id, DivisionReceivingDTO divisionReceiving)
        {
            var response = await _divisonService.UpdateDivision(id, divisionReceiving);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var division = ((ApiOkResponse)response).Result;
            return Ok((DivisionTransferDTO)division);
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteById(int id)
        {
            var response = await _divisonService.DeleteDivision(id);
            return StatusCode(response.StatusCode);
        }
    }
}