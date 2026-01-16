using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using CIS.API.DataTransferObjects.EncounterType.Request;
using CIS.API.DataTransferObjects.EncounterType.Response;
using CIS.Common.Exceptions;
using CIS.ServiceContracts.ServiceObjects;
using CIS.ServiceContracts.Services;
using Microsoft.AspNetCore.Mvc;

namespace CIS.API.Controllers
{
    [ApiController]
    [Route("api/v1/[controller]")]
    [Produces("application/json")]

    public class EncounterTypeController : ApiController
    {
        private readonly IEncounterTypeService _encounterTypeService;

        public EncounterTypeController(
            IEncounterTypeService encounterTypeService,
             IMapper mapper) : base(mapper)
        {
            _encounterTypeService = encounterTypeService;
        }

        [HttpPost]
        [ProducesResponseType(201, Type = typeof(AddEncounterTypeRequestDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> CreateEncounterTypeAsync(
           [FromBody] AddEncounterTypeRequestDto addEncounterTypeRequestDto,
           CancellationToken token = default)
        {
            try
            {
                var encounterTypeServiceObject = Mapper.Map<EncounterTypeServiceObject>(addEncounterTypeRequestDto);
                var serviceResult = await _encounterTypeService.CreateEncounterTypeAsync(encounterTypeServiceObject, token);
                return new CreatedResult(string.Empty, Mapper.Map<AddEncounterTypeResponseDto>(serviceResult));
            }
            catch (BadRequestException e)
            {
                return new BadRequestObjectResult(e.Error);
            }
        }
        [HttpGet]
        [ProducesResponseType(200, Type = typeof(GetEncounterTypeResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetEncounterTypeListAsync(
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _encounterTypeService.GetEncounterTypeListAsync(token);
                return new OkObjectResult(Mapper.Map<IEnumerable<GetEncounterTypeResponseDto>>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpGet("{encounterTypeId}")]
        [ProducesResponseType(200, Type = typeof(GetEncounterTypeResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetEncounterTypeByIdAsync(
            [FromRoute] int encounterTypeId,
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _encounterTypeService.GetEncounterTypeByIdAsync(encounterTypeId, token);
                return new OkObjectResult(Mapper.Map<GetEncounterTypeResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }

        [HttpPut("{encounterTypeId}")]
        [ProducesResponseType(200, Type = typeof(UpdateEncounterTypeResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> UpdateEncounterTypeByIdAsync(
            [FromRoute] int encounterTypeId,
            [FromBody] UpdateEncounterTypeRequestDto updateEncounterTypeRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var encounterTypeServiceObject = Mapper.Map<EncounterTypeServiceObject>(updateEncounterTypeRequestDto);
                encounterTypeServiceObject.Id = encounterTypeId;
                var serviceResult = await _encounterTypeService.UpdateEncounterTypeByIdAsync(encounterTypeServiceObject, token);
                return new OkObjectResult(Mapper.Map<UpdateEncounterTypeResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpDelete("{encounterTypeId}")]
        [ProducesResponseType(204)]
        [ProducesResponseType(404)]
        public async Task<IActionResult> DeleteEncounterTypeByIdAsync(
            [FromRoute] int encounterTypeId,
            CancellationToken token = default)
        {
            try
            {
                await _encounterTypeService.DeleteEncounterTypeByIdAsync(encounterTypeId, token);
                return new StatusCodeResult(204);
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }
    }
}