using App.Controllers.Base;
using App.Models.DTOs.UpdateRquests;
using App.Models.DTOs;
using App.Services;
using App.Services.Interface;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using App.Models.DTOs.CreateRequests;

namespace App.Controllers
{
    public class CommandController : ApiController
    {
        private readonly ICommandService _commandService;
        public CommandController(ILogger<CommandController> logger, ICommandService commandService) : base(logger)
        {
            _commandService = commandService;
        }
        [HttpPost]
        public async Task<ActionResult> Post([FromBody] CommandCreateRequest request)
        {
            if (null == request)
                return BadRequest();
            var result = await _commandService.CreateAsync(request);

            if (null == result)
                return StatusCode(StatusCodes.Status500InternalServerError);
            return Ok(result);
        }
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(string id, CommandUpdateRequest request)
        {
            if (id != request.Id)
                return BadRequest();
            var result = await _commandService.UpdateAsync(id, request);
            if (result > 0)
                return Ok();
            return NotFound();
        }
        [HttpDelete("{id}")]
        public async Task<ActionResult> Delete(string id, CommandUpdateRequest request)
        {
            if (id != request.Id)
                return BadRequest();
            var result = await _commandService.DeleteSoftAsync(id);
            if (result > 0)
                return Ok();
            return NotFound();
        }
        [HttpGet("")]
        public async Task<ActionResult> GetAll( string searchText = "")
        {
            var result = await _commandService.GetAllDTOAsync();
            return Ok(result);
        }

        [HttpGet("{id}")]
        public async Task<ActionResult> GetById(string id)
        {
            var result = await _commandService.GetByIdAsync(id);

            if (result != null)
                return Ok(result);
            return NotFound();
        }

        [HttpGet("filter")]
        public async Task<ActionResult> GetPaging(int pageIndex, int pageSize, string searchText = "")
        {
            try
            {
                throw new NotImplementedException();
                //var result = await _commandService.GetDetailsPagingAsync(pageIndex, pageSize, searchText);
                //return Ok(result);
            }
            catch (ArgumentOutOfRangeException ex)
            {
                _logger.LogError(ex.Message);
                return BadRequest(ex.Message);
            }
            catch(Exception ex)
            {
                _logger.LogError(ex.StackTrace);
                _logger.LogError(ex.Message);
                return StatusCode(StatusCodes.Status500InternalServerError);
            }
        }
    }
}