using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WaterSolutionAPI.Interfaces;
using WaterSolutionAPI.Models;

namespace WaterSolutionAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class RoleController : ControllerBase
    {
        private readonly IRole _role;

        public RoleController(IRole role)
        {
            _role = role;
        }

        // POST: api/Role
        [HttpPost]
        [Route("save")]
        public async Task<ActionResult> Post([FromBody] Role model)
        {
            if (!ModelState.IsValid) return BadRequest();

            await _role.save(model);

            return Ok(model);
        }

        // PUT: api/Role/5
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(int id, [FromBody] Role model)
        {
            if (id != model.IdRole) return BadRequest();

            try
            {
                await _role.Edit(model);
            }
            catch (Exception ex)
            {
                if (!Exists(id)) return BadRequest();
                throw ex;
            }
            return Ok(model);
        }

        private bool Exists(int id) => _role.Exists(id);
      
    }
}