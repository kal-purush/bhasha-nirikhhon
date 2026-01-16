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
    public class BranchController : ControllerBase
    {
        private readonly IBranchService _branchService;

        public BranchController(IBranchService branchService)
        {
            this._branchService = branchService;
        }

        [HttpGet("")]
        public async Task<ActionResult> GetBranches()
        {
            var response = await _branchService.GetAllBranches();
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var branch = ((ApiOkResponse)response).Result;
            return Ok((IEnumerable<BranchTransferDTO>)branch);
        }
        [HttpGet("name/{name}")]
        public async Task<ActionResult> GetByName(string name)
        {
            var response = await _branchService.GetBranchByName(name);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var branch = ((ApiOkResponse)response).Result;
            return Ok((BranchTransferDTO)branch);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult> GetById(long id)
        {
            var response = await _branchService.GetBranchById(id);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var branch = ((ApiOkResponse)response).Result;
            return Ok((BranchTransferDTO)branch);
        }

        [HttpPost("")]
        public async Task<ActionResult> AddNewBranch(BranchReceivingDTO branchReceiving)
        {
            var response = await _branchService.AddBranch(branchReceiving);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var branch = ((ApiOkResponse)response).Result;
            return Ok((BranchTransferDTO)branch);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateById(long id, BranchReceivingDTO branchReceiving)
        {
            var response = await _branchService.UpdateBranch(id, branchReceiving);
            if (response.StatusCode >= 400)
                return StatusCode(response.StatusCode, response);
            var branch = ((ApiOkResponse)response).Result;
            return Ok((BranchTransferDTO)branch);
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult> DeleteById(int id)
        {
            var response = await _branchService.DeleteBranch(id);
            return StatusCode(response.StatusCode);
        }
    }
}