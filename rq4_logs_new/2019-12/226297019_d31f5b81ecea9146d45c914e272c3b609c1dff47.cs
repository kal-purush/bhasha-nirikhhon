using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using CIS.API.DataTransferObjects.Appointment.Request;
using CIS.API.DataTransferObjects.Appointment.Response;
using CIS.Common.Exceptions;
using CIS.ServiceContracts.ServiceObjects;
using CIS.ServiceContracts.Services;
using Microsoft.AspNetCore.Mvc;

namespace CIS.API.Controllers
{
    [ApiController]
    [Route("api/v1/[controller]")]
    [Produces("application/json")]
    public class AppointmentController : ApiController
    {
        private readonly IAppointmentService _appointmentService;

        public AppointmentController(
            IAppointmentService appointmentService,
             IMapper mapper) : base(mapper)
        {
            _appointmentService = appointmentService;
        }

        [HttpPost]
        [ProducesResponseType(201, Type = typeof(AddAppointmentResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> CreateAppointmentAsync(
            [FromBody] AddAppointmentRequestDto addAppointmentRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var appointmentServiceObject = Mapper.Map<AppointmentServiceObject>(addAppointmentRequestDto);
                var serviceResult = await _appointmentService.CreateAppointmentAsync(appointmentServiceObject, token);
                return new CreatedResult(string.Empty, Mapper.Map<AddAppointmentResponseDto>(serviceResult));
            }
            catch (BadRequestException e)
            {
                return new BadRequestObjectResult(e.Error);
            }
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(GetAppointmentResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetAppointmentListAsync(
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _appointmentService.GetAppointmentListAsync(token);
                return new OkObjectResult(Mapper.Map<IEnumerable<GetAppointmentResponseDto>>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpGet("{appointmentId}")]
        [ProducesResponseType(200, Type = typeof(GetAppointmentResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetAppointmentByIdAsync(
            [FromRoute] int appointmentId,
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _appointmentService.GetAppointmentByIdAsync(appointmentId, token);
                return new OkObjectResult(Mapper.Map<GetAppointmentResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }

        [HttpPut("{appointmentId}")]
        [ProducesResponseType(200, Type = typeof(UpdateAppointmentResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> UpdateAppointmentByIdAsync(
            [FromRoute] int appointmentId,
            [FromBody] UpdateAppointmentRequestDto updateAppointmentRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var appointmentServiceObject = Mapper.Map<AppointmentServiceObject>(updateAppointmentRequestDto);
                appointmentServiceObject.Id = appointmentId;
                var serviceResult = await _appointmentService.UpdateAppointmentByIdAsync(appointmentServiceObject, token);
                return new OkObjectResult(Mapper.Map<UpdateAppointmentResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpDelete("{appointmentId}")]
        [ProducesResponseType(204)]
        [ProducesResponseType(404)]
        public async Task<IActionResult> DeleteAppointmentByIdAsync(
            [FromRoute] int appointmentId,
            CancellationToken token = default)
        {
            try
            {
                await _appointmentService.DeleteAppointmentByIdAsync(appointmentId, token);
                return new StatusCodeResult(204);
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }
    }
}