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
    public class OfficeController : ControllerBase
    {
        private readonly IOfficeService _officeService;

        public OfficeController(IOfficeService officeService)
        {
            this._officeService = officeService;
        }

        [HttpGet("")]
        public async Task<ActionResult> GetOffices()
        {
            var response = await _officeService.GetAllOffices();
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var offices = ((ApiOkResponse)response).Result;
            return Ok(offices);
        }

        [HttpGet("name/{name}")]
        public async Task<ActionResult> GetByName(string name)
        {
            var response = await _officeService.GetOfficeByName(name);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var office = ((ApiOkResponse)response).Result;
            return Ok(office);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult> GetById(long id)
        {
            var response = await _officeService.GetOfficeById(id);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var office = ((ApiOkResponse)response).Result;
            return Ok(office);
        }

        [HttpPost("")]
        public async Task<ActionResult> AddNewOffice(OfficeReceivingDTO officeReceivingDTO)
        {
            var response = await _officeService.AddOffice(officeReceivingDTO);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var office = ((ApiOkResponse)response).Result;
            return Ok(office);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateById(long id, OfficeReceivingDTO officeReceivingDTO)
        {
            var response = await _officeService.UpdateOffice(id, officeReceivingDTO);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var office = ((ApiOkResponse)response).Result;
            return Ok(office);
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteById(int id)
        {
            var response = await _officeService.DeleteOffice(id);
            return StatusCode(response.StatusCode);
        }
    }
}