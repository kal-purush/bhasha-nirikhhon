using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using CIS.API.DataTransferObjects.Regulatory.Request;
using CIS.API.DataTransferObjects.Regulatory.Response;
using CIS.Common.Exceptions;
using CIS.ServiceContracts.ServiceObjects;
using CIS.ServiceContracts.Services;
using Microsoft.AspNetCore.Mvc;

namespace CIS.API.Controllers
{
    [ApiController]
    [Route("api/v1/[controller]")]
    [Produces("application/json")]
    
    public class RegulatoryController : ApiController
    {
        private readonly IRegulatoryService _regulatoryService;

        public RegulatoryController(
            IRegulatoryService regulatoryService,
             IMapper mapper) : base(mapper)
        {
            _regulatoryService = regulatoryService;
        }
        [HttpPost]
        [ProducesResponseType(201, Type = typeof(AddRegulatoryResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> CreateRegulatoryAsync(
           [FromBody] AddRegulatoryRequestDto addRegulatoryRequestDto,
           CancellationToken token = default)
        {
            try
            {
                var regulatoryServiceObject = Mapper.Map<RegulatoryServiceObject>(addRegulatoryRequestDto);
                var serviceResult = await _regulatoryService.CreateRegulatoryAsync(regulatoryServiceObject, token);
                return new CreatedResult(string.Empty, Mapper.Map<AddRegulatoryResponseDto>(serviceResult));
            }
            catch (BadRequestException e)
            {
                return new BadRequestObjectResult(e.Error);
            }
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(GetRegulatoryResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetRegulatoryListAsync(
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _regulatoryService.GetRegulatoryListAsync(token);
                return new OkObjectResult(Mapper.Map<IEnumerable<GetRegulatoryResponseDto>>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpGet("{regulatoryId}")]
        [ProducesResponseType(200, Type = typeof(GetRegulatoryResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetRegulatoryByIdAsync(
            [FromRoute] int regulatoryId,
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _regulatoryService.GetRegulatoryByIdAsync(regulatoryId, token);
                return new OkObjectResult(Mapper.Map<GetRegulatoryResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }

        [HttpPut("{regulatoryId}")]
        [ProducesResponseType(200, Type = typeof(UpdateRegulatoryResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> UpdateRegulatoryByIdAsync(
            [FromRoute] int regulatoryId,
            [FromBody] UpdateRegulatoryRequestDto updateRegulatoryRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var regulatoryServiceObject = Mapper.Map<RegulatoryServiceObject>(updateRegulatoryRequestDto);
                regulatoryServiceObject.Id = regulatoryId;
                var serviceResult = await _regulatoryService.UpdateRegulatoryByIdAsync(regulatoryServiceObject, token);
                return new OkObjectResult(Mapper.Map<UpdateRegulatoryResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpDelete("{regulatoryId}")]
        [ProducesResponseType(204)]
        [ProducesResponseType(404)]
        public async Task<IActionResult> DeleteRegulatoryByIdAsync(
            [FromRoute] int regulatoryId,
            CancellationToken token = default)
        {
            try
            {
                await _regulatoryService.DeleteRegulatoryByIdAsync(regulatoryId, token);
                return new StatusCodeResult(204);
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }
    }
}