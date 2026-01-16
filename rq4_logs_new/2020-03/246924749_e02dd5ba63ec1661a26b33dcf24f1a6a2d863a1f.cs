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
    public class UsuarioController : ControllerBase
    {
        private readonly IUsuario _usuario;
        // POST: api/Usuario
        [HttpPost]
        public async Task<ActionResult> Post([FromBody] Usuarios model)
        {
            if (ModelState.IsValid) return BadRequest();

            await _usuario.save(model);

            return Ok(model);
        }

        // PUT: api/Usuario/5
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(int id, [FromBody] Usuarios model)
        {
            if (id != model.IdUsuario) return BadRequest();

            try
            {
                await _usuario.Edit(model);
            }
            catch (Exception ex)
            {
                if (!Exists(id)) return BadRequest();
                throw ex;
            }
            return Ok(model);
        }

        private bool Exists(int id) => _usuario.Exists(id);
       
    }
}