using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using CIS.API.DataTransferObjects.Facilities.Request;
using CIS.API.DataTransferObjects.Facilities.Response;
using CIS.Common.Exceptions;
using CIS.ServiceContracts.ServiceObjects;
using CIS.ServiceContracts.Services;
using Microsoft.AspNetCore.Mvc;

namespace CIS.API.Controllers
{
    [ApiController]
    [Route("api/v1/[controller]")]
    [Produces("application/json")]

    public sealed class FacilityController : ApiController
    {
        private readonly IFacilityService _facilityService;

        public FacilityController(
            IFacilityService faciltyService,
             IMapper mapper) : base(mapper)
        {
            _facilityService = faciltyService;
        }

        [HttpPost]
        [ProducesResponseType(201, Type = typeof(AddFacilityResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> CreateFacilityAsync(
            [FromBody] AddFacilityRequestDto addFacilityRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var facilityServiceObject = Mapper.Map<FacilityServiceObject>(addFacilityRequestDto);
                var serviceResult = await _facilityService.CreateFacilityAsync(facilityServiceObject, token);
                return new CreatedResult(string.Empty, Mapper.Map<AddFacilityResponseDto>(serviceResult));
            }
            catch (BadRequestException e)
            {
                return new BadRequestObjectResult(e.Error);
            }
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(GetFacilityResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetFacilityListAsync(
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _facilityService.GetFacilityListAsync(token);
                return new OkObjectResult(Mapper.Map<IEnumerable<GetFacilityResponseDto>>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpGet("{facilityId}")]
        [ProducesResponseType(200, Type = typeof(GetFacilityResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetFacilityByIdAsync(
            [FromRoute] int facilityId,
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _facilityService.GetFacilityByIdAsync(facilityId, token);
                return new OkObjectResult(Mapper.Map<GetFacilityResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }

        [HttpPut("{facilityId}")]
        [ProducesResponseType(200, Type = typeof(UpdateFacilityResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> UpdateFacilityByIdAsync(
            [FromRoute] int facilityId,
            [FromBody] UpdateFacilityRequestDto updateFacilityRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var facilityServiceObject = Mapper.Map<FacilityServiceObject>(updateFacilityRequestDto);
                facilityServiceObject.Id = facilityId;
                var serviceResult = await _facilityService.UpdateFacilityByIdAsync(facilityServiceObject, token);
                return new OkObjectResult(Mapper.Map<UpdateFacilityResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpDelete("{facilityId}")]
        [ProducesResponseType(204)]
        [ProducesResponseType(404)]
        public async Task<IActionResult> DeleteFacilityByIdAsync(
            [FromRoute] int facilityId,
            CancellationToken token = default)
        {
            try
            {
                await _facilityService.DeleteFacilityByIdAsync(facilityId, token);
                return new StatusCodeResult(204);
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }
    }
}