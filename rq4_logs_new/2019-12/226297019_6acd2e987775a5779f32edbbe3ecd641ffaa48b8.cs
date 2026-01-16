using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using CIS.ServiceContracts.Services;
using FluentValidation;
using Microsoft.AspNetCore.Mvc;
using System.Threading;
using CIS.Common.Exceptions;
using CIS.Common.Extensions;
using CIS.ServiceContracts.ServiceObjects;
using CIS.API.DataTransferObjects.Companies.Request;
using CIS.API.DataTransferObjects.Companies.Response;

namespace CIS.API.Controllers
{
    [ApiController]
    [Route("api/v1/[controller]")]
    [Produces("application/json")]
    public sealed class CompanyController : ApiController
    {
        private readonly ICompanyService _companyService;

        public CompanyController(
            ICompanyService companyService,
             IMapper mapper) : base(mapper)
        {
            _companyService = companyService;
        }

        [HttpPost]
        [ProducesResponseType(201, Type = typeof(AddCompanyResponseDto))]
        [ProducesResponseType(400)]
        public async Task<IActionResult> CreateCompanyAsync(
            [FromBody] AddCompanyRequestDto addCompanyRequestDto,
            CancellationToken token = default)
        {
            try
            {
                var companyServiceObject = Mapper.Map<CompanyServiceObject>(addCompanyRequestDto);
                var serviceResult = await _companyService.CreateCompanyAsync(companyServiceObject, token);
                return new CreatedResult(string.Empty, Mapper.Map<AddCompanyResponseDto>(serviceResult));
            }
            catch (BadRequestException e)
            {
                return new BadRequestObjectResult(e.Error);
            }
        }

        [HttpGet]
        [ProducesResponseType(200, Type = typeof(GetCompanyResponseDto))]
        [ProducesResponseType(404)]
        public async Task<IActionResult> GetCompanyListAsync(
            CancellationToken token = default)
        {
            try
            {
                var serviceResult = await _companyService.GetCompanyListAsync(token);
                return new OkObjectResult(Mapper.Map<IEnumerable<GetCompanyResponseDto>>(serviceResult));
            }
            catch (Exception)
            {
                return new BadRequestResult();
            }
        }
    }
}