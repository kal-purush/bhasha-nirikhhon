using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WaterSolutionAPI.Models;
using WaterSolutionAPI.Interfaces;

namespace WaterSolutionAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class RolePermisosController : ControllerBase
    {
        private readonly IRolePermisos _rolePermisos;

        public RolePermisosController(IRolePermisos rolePermisos)
        {
            _rolePermisos = rolePermisos;
        }

        // POST: api/RolePermisos
        [HttpPost]
        public async Task<ActionResult> Post([FromBody] RolesPermisos model)
        {
            if (!ModelState.IsValid) return BadRequest();

            await _rolePermisos.save(model);

            return Ok(model);

        }

        // PUT: api/RolePermisos/5
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(int id, [FromBody] RolesPermisos model)
        {
            if (id != model.IdPermiso) return BadRequest();

            try
            {
                await _rolePermisos.Edit(model);
            }
            catch (Exception ex)
            {
                if (!Exists(id)) return BadRequest();
                throw ex;
            }
            return Ok(model);
        }

        private bool Exists(int id) => _rolePermisos.Exists(id);
       
    }
}