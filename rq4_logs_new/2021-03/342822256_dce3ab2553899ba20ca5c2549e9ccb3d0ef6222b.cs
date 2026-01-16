using HostelWebAPI.DataAccess.Interfaces;
using HostelWebAPI.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace HostelWebAPI.Controllers
{
    [Produces("application/json")]
    [Route("api/properties")]
    [ApiController]
    public class PropertiesController : ControllerBase
    {
        private readonly IDbRepo repo;

        public PropertiesController(IDbRepo repo)
        {
            this.repo = repo;
        }

        // GET: api/<PropertyController>
        [HttpGet]
        public async Task<IActionResult> GetAll([FromQuery] string typeId)
        {
            var properties = await repo.Properties.GetAllAsync();
            var resp = new List<PropertyViewResponse>();

            if (typeId != null)
                properties = properties.Where(r => r.PropertyTypeId == typeId).ToList();
                
            resp = properties.Select(p => new PropertyViewResponse(p)).ToList();

            return Ok(resp);
        }

        // GET api/<PropertyController>/5
        [HttpGet("{id}")]
        public async Task<IActionResult> Get(string id)
        {
            var property = await repo.Properties.GetByIdAsync(id);

            var resp = new PropertyResponse(property);

            if (property != null) return Ok(resp);
            else return BadRequest();
        }

        // POST api/<PropertyController>
        [HttpPost]
        [Authorize(Roles = AppRoles.Owner)]
        public async Task<IActionResult> Post()
        {
            return Ok("Hello owner");
        }
    }
}