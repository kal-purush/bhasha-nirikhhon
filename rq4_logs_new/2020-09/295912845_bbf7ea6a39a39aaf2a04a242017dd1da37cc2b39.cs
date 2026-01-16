using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using OffiRent.API.Domain.Models;
using OffiRent.API.Domain.Services;
using OffiRent.API.Extensions;
using OffiRent.API.Resources;

namespace OffiRent.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DepartamentController : ControllerBase
    {
        private readonly IDepartamentService _departamentService;

        private readonly IMapper _mapper;

        public DepartamentController(IDepartamentService departamentService, IMapper mapper)
        {
            _departamentService = departamentService;
            _mapper = mapper;
        }

        [HttpGet]
        public async Task<IEnumerable<DepartamentResource>> GetAllAsync()
        {
            var departaments = await _departamentService.ListAsync();
            var resources = _mapper.Map<IEnumerable<Departament>, IEnumerable<DepartamentResource>>(departaments);
            return resources;
        }

        [HttpPost]
        public async Task<IActionResult> PostAsync([FromBody] SaveDepartamentResource resource)
        {
            if (!ModelState.IsValid)
                return BadRequest(ModelState.GetErrorMessages());

            var departament = _mapper.Map<SaveDepartamentResource, Departament>(resource);
            var result = await _departamentService.SaveAsync(departament);

            if (!result.Success)
                return BadRequest(result.Message);

            var departamentResource = _mapper.Map<Departament, DepartamentResource>(result.Resource);

            return Ok(departamentResource);

        }
    }
}
