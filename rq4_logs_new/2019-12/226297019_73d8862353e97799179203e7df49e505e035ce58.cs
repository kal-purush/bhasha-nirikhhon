using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using CIS.API.DataTransferObjects.MedicalRecord.Request;
using CIS.API.DataTransferObjects.MedicalRecord.Response;
using CIS.Common.Exceptions;
using CIS.ServiceContracts.ServiceObjects;
using CIS.ServiceContracts.Services;
using Microsoft.AspNetCore.Mvc;

namespace CIS.API.Controllers
{
    [ApiController]
    [Route("api/v1/[controller]")]
    [Produces("application/json")]

    public class MedicalRecordController : ApiController
    {
        private readonly IMedicalRecordService _medicalRecordService;

        public MedicalRecordController(
            IMedicalRecordService medicalRecordService,
             IMapper mapper) : base(mapper)
        {
            _medicalRecordService = medicalRecordService;
        }
        [HttpPost]
        [ProducesResponseType(201, Type = typeof(AddMedicalRecordResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> CreateMedicalRecordAsync(
           [FromBody] AddMedicalRecordRequestDto addMedicalRecordRequestDto,
           CancellationToken token = default)
        {
            try
            {
                var medicalRecordServiceObject = Mapper.Map<MedicalRecordServiceObject>(addMedicalRecordRequestDto);
                var serviceResult = await _medicalRecordService.CreateMedicalRecordAsync(medicalRecordServiceObject, token);
                return new CreatedResult(string.Empty, Mapper.Map<AddMedicalRecordResponseDto>(serviceResult));
            }
            catch (BadRequestException e)
            {
                return new BadRequestObjectResult(e.Error);
            }
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(GetMedicalRecordResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetMedicalRecordListAsync(
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _medicalRecordService.GetMedicalRecordListAsync(token);
                return new OkObjectResult(Mapper.Map<IEnumerable<GetMedicalRecordResponseDto>>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpGet("{medicalRecordId}")]
        [ProducesResponseType(200, Type = typeof(GetMedicalRecordResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetMedicalRecordByIdAsync(
            [FromRoute] int medicalRecordId,
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _medicalRecordService.GetMedicalRecordByIdAsync(medicalRecordId, token);
                return new OkObjectResult(Mapper.Map<GetMedicalRecordResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }

        [HttpPut("{medicalRecordId}")]
        [ProducesResponseType(200, Type = typeof(UpdateMedicalRecordResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> UpdateMedicalRecordByIdAsync(
            [FromRoute] int medicalRecordId,
            [FromBody] UpdateMedicalRecordRequestDto updateMedicalRecordRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var medicalRecordServiceObject = Mapper.Map<MedicalRecordServiceObject>(updateMedicalRecordRequestDto);
                medicalRecordServiceObject.Id = medicalRecordId;
                var serviceResult = await _medicalRecordService.UpdateMedicalRecordByIdAsync(medicalRecordServiceObject, token);
                return new OkObjectResult(Mapper.Map<UpdateMedicalRecordResponseDto>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }

        [HttpDelete("{medicalRecordId}")]
        [ProducesResponseType(204)]
        [ProducesResponseType(404)]
        public async Task<IActionResult> DeleteMedicalRecordByIdAsync(
            [FromRoute] int medicalRecordId,
            CancellationToken token = default)
        {
            try
            {
                await _medicalRecordService.DeleteMedicalRecordByIdAsync(medicalRecordId, token);
                return new StatusCodeResult(204);
            }
            catch (Exception)
            {
                return new NotFoundResult();
            }
        }
    }
}