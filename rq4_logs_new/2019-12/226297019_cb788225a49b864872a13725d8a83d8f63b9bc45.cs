using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using CIS.API.DataTransferObjects.ActivityTypes.Request;
using CIS.API.DataTransferObjects.ActivityTypes.Response;
using CIS.Common.Exceptions;
using CIS.ServiceContracts.ServiceObjects;
using CIS.ServiceContracts.Services;
using Microsoft.AspNetCore.Mvc;

namespace CIS.API.Controllers
{
    [ApiController]
    [Route("api/v1/[controller]")]
    [Produces("application/json")]
    public sealed class ActivityTypeController : ApiController
    {
        private readonly IActivityTypeService _activityTypeService;

        public ActivityTypeController(
            IActivityTypeService activityTypeService,
             IMapper mapper) : base(mapper)
        {
            _activityTypeService = activityTypeService;
        }

        [HttpPost]
        [ProducesResponseType(201, Type = typeof(AddActivityTypeResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> CreateActivityTypeAsync(
            [FromBody] AddActivityTypeRequestDto addActivityTypeRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var activityTypeServiceObject = Mapper.Map<ActivityTypeServiceObject>(addActivityTypeRequestDto);
                var serviceResult = await _activityTypeService.CreateActivityTypeAsync(activityTypeServiceObject, token);
                return new CreatedResult(string.Empty, Mapper.Map<AddActivityTypeResponseDto>(serviceResult));
            }
            catch (BadRequestException e)
            {
                return new BadRequestObjectResult(e.Error);
            }
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(GetActivityTypeResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetActivityTypeListAsync(
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _activityTypeService.GetActivityTypeListAsync(token);
                return new OkObjectResult(Mapper.Map<IEnumerable<GetActivityTypeResponseDto>>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpGet("{activityTypeId}")]
        [ProducesResponseType(200, Type = typeof(GetActivityTypeResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetActivityTypeByIdAsync(
            [FromRoute] int activityTypeId,
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _activityTypeService.GetActivityTypeByIdAsync(activityTypeId, token);
                return new OkObjectResult(Mapper.Map<GetActivityTypeResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }

        [HttpPut("{activityTypeId}")]
        [ProducesResponseType(200, Type = typeof(UpdateActivityTypeResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> UpdateActivityTypeByIdAsync(
            [FromRoute] int activityTypeId,
            [FromBody] UpdateActivityTypeRequestDto updateActivityTypeRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var activityTypeServiceObject = Mapper.Map<ActivityTypeServiceObject>(updateActivityTypeRequestDto);
                activityTypeServiceObject.Id = activityTypeId;
                var serviceResult = await _activityTypeService.UpdateActivityTypeByIdAsync(activityTypeServiceObject, token);
                return new OkObjectResult(Mapper.Map<UpdateActivityTypeResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpDelete("{activityTypeId}")]
        [ProducesResponseType(204)]
        [ProducesResponseType(404)]
        public async Task<IActionResult> DeleteActivityTypeByIdAsync(
            [FromRoute] int activityTypeId,
            CancellationToken token = default)
        {
            try
            {
                await _activityTypeService.DeleteActivityTypeByIdAsync(activityTypeId, token);
                return new StatusCodeResult(204);
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }
    }
}