using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Supermarket.API.Domain.Models;
using Supermarket.API.Domain.Services;
using Supermarket.API.Extensions;
using Supermarket.API.Resources;

namespace Supermarket.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CountryController : ControllerBase
    {
        private readonly ICountryService _countryService;

        private readonly IMapper _mapper;

        public CountryController(ICountryService countryService, IMapper mapper)
        {
            _countryService = countryService;
            _mapper = mapper;
        }

        [HttpGet]
        public async Task<IEnumerable<CountryResource>> GetAllAsync()
        {
            var countries = await _countryService.ListAsync();
            var resources = _mapper.Map<IEnumerable<Country>, IEnumerable<CountryResource>>(countries);
            return resources;
        }

        [HttpPost]
        public async Task<IActionResult> PostAsync([FromBody] SaveCountryResource resource)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState.GetErrorMessages());

            var country = _mapper.Map<SaveCountryResource, Country>(resource);
            var result = await _countryService.SaveAsync(country);

            if (!result.Success)
                return BadRequest(result.Message);

            var countryResource = _mapper.Map<Country, CountryResource>(result.Resource);

            return Ok(countryResource);

        }
    }
}