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
    public class PermisoRoleController : ControllerBase
    {
        private readonly IPermisoRole _permisoRole;

        public PermisoRoleController(IPermisoRole permisoRole)
        {
            _permisoRole = permisoRole;
        }

        // POST: api/PermisoRole
        [HttpPost]
        public async Task<ActionResult> Post([FromBody] PermisoRole model)
        {
            if (!ModelState.IsValid) return BadRequest();

            await _permisoRole.save(model);

            return Ok(model);
        }

        // PUT: api/PermisoRole/5
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(int id, [FromBody] PermisoRole model)
        {
            if (id != model.IdPermiso) return BadRequest();

            try
            {
                await _permisoRole.Edit(model);
            }
            catch (Exception ex)
            {
                if (Exists(id)) return BadRequest();
                throw ex;
            }
            return Ok(model);
        }

        private bool Exists(int id) => _permisoRole.Exists(id);
    }
}