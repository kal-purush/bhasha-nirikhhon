using AutoMapper;
using BestUzdNew.Api.Dtos.Input;
using BestUzdNew.Api.Dtos.Output;
using BestUzdNew.Domain.Entities;
using BestUzdNew.Logic;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BestUzdNew.Api.Controllers
{
    [Route("api/service")]
    [ApiController]
    public class ServiceController : ControllerBase
    {
        private readonly IMapper _mapper;
        private readonly IServiceForServiceEntity _service;

        public ServiceController(IMapper mapper, IServiceForServiceEntity service)
        {
            _mapper = mapper;
            _service = service;
        }

        [HttpPost]
        public async Task<IActionResult> CreateService(ServiceInDto dto)
        {
            var service = _mapper.Map<ServiceInDto, Service>(dto);
            var nameTranslation = _mapper.Map<TranslationInDto, Translation>(dto.NameTranslationDto);
            var descriptionTranslation = _mapper.Map<TranslationInDto, Translation>(dto.DescriptionTranslationDto);

            await _service.CreateServiceAsync(service, nameTranslation, descriptionTranslation);

            return Ok();
        }

        [HttpGet]
        public async Task<IActionResult> GetServices()
        {
            var services = await _service.GetServicesAsync();
            var dtos = _mapper.Map<IEnumerable<Service>, IEnumerable<ServiceOutDto>>(services);

            return Ok(dtos);
        }
    }
}