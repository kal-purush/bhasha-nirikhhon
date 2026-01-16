using HostelWebAPI.DataAccess.Interfaces;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace HostelWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CityController : ControllerBase
    {
        private readonly IDbRepo repo;

        public CityController(IDbRepo repo)
        {
            this.repo = repo;
        }
        // GET: api/<CityController>
        [HttpGet]
        public async Task<ActionResult<IEnumerable<CityResponse>>> Get()
        {
            var cities = await repo.Cities.GetAllAsync();

            var resp = new List<CityResponse>(cities.Select(c => new CityResponse(c)).ToList());

            return Ok(resp);
        }

        public class CityResponse
        {
            public CityResponse(Models.City c)
            {
                CityId = c.CityId;
                CityName = c.Name;
                CountryId = c.CountryId;
            }

            public string CityId { get; set; }
            public string CityName { get; set; }
            public string CountryId { get; set; }
            public string CountryName { get; set; }
        }
    }
}