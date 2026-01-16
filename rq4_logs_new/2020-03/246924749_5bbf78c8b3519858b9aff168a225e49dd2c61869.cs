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
    public class SeccioneController : ControllerBase
    {
        private readonly ISecciones _secciones;

        public SeccioneController(ISecciones secciones)
        {
            _secciones = secciones;
        }

        // POST: api/Seccione
        [HttpPost]
        public async Task<ActionResult> Post([FromBody] Secciones model)
        {
            if (!ModelState.IsValid) return BadRequest();

            await _secciones.save(model);

            return Ok(model);
        }

        // PUT: api/Seccione/5
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(int id, [FromBody] Secciones model)
        {
            if (id != model.IdSeccion) return BadRequest();

            try
            {
                await _secciones.Edit(model);
            }
            catch (Exception ex)
            {
                if (!Exists(id)) return BadRequest();
                throw ex;
            }
            return Ok(model);
        }

        private bool Exists(int id) => _secciones.Exists(id);
    }
}