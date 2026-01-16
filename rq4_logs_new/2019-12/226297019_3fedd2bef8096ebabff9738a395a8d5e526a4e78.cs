using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using CIS.API.DataTransferObjects.Registration.Request;
using CIS.API.DataTransferObjects.Registration.Response;
using CIS.Common.Exceptions;
using CIS.ServiceContracts.ServiceObjects;
using CIS.ServiceContracts.Services;
using Microsoft.AspNetCore.Mvc;

namespace CIS.API.Controllers
{
    [ApiController]
    [Route("api/v1/[controller]")]
    [Produces("application/json")]
    public class RegistrationController : ApiController
    {
        private readonly IRegistrationService _registrationService;

        public RegistrationController(
            IRegistrationService registrationService,
             IMapper mapper) : base(mapper)
        {
            _registrationService = registrationService;
        }

        [HttpPost]
        [ProducesResponseType(201, Type = typeof(AddRegistrationResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> CreateRegistrationAsync(
            [FromBody] AddRegistrationRequestDto addRegistrationRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var registrationServiceObject = Mapper.Map<RegistrationServiceObject>(addRegistrationRequestDto);
                var serviceResult = await _registrationService.CreateRegistrationAsync(registrationServiceObject, token);
                return new CreatedResult(string.Empty, Mapper.Map<AddRegistrationResponseDto>(serviceResult));
            }
            catch (BadRequestException e)
            {
                return new BadRequestObjectResult(e.Error);
            }
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(GetRegistrationResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetRegistrationListAsync(
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _registrationService.GetRegistrationListAsync(token);
                return new OkObjectResult(Mapper.Map<IEnumerable<GetRegistrationResponseDto>>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpGet("{registrationId}")]
        [ProducesResponseType(200, Type = typeof(GetRegistrationResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetRegistrationByIdAsync(
            [FromRoute] int registrationId,
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _registrationService.GetRegistrationByIdAsync(registrationId, token);
                return new OkObjectResult(Mapper.Map<GetRegistrationResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }

        [HttpPut("{registrationId}")]
        [ProducesResponseType(200, Type = typeof(UpdateRegistrationResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> UpdateRegistrationByIdAsync(
            [FromRoute] int registrationId,
            [FromBody] UpdateRegistrationRequestDto updateRegistrationRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var registrationServiceObject = Mapper.Map<RegistrationServiceObject>(updateRegistrationRequestDto);
                registrationServiceObject.Id = registrationId;
                var serviceResult = await _registrationService.UpdateRegistrationByIdAsync(registrationServiceObject, token);
                return new OkObjectResult(Mapper.Map<UpdateRegistrationResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpDelete("{registrationId}")]
        [ProducesResponseType(204)]
        [ProducesResponseType(404)]
        public async Task<IActionResult> DeleteRegistrationByIdAsync(
            [FromRoute] int registrationId,
            CancellationToken token = default)
        {
            try
            {
                await _registrationService.DeleteRegistrationByIdAsync(registrationId, token);
                return new StatusCodeResult(204);
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }
    }
}