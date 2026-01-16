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
    public class RutaController : ControllerBase
    {
        private readonly IRuta _ruta;

        public RutaController(IRuta ruta)
        {
            _ruta = ruta;
        }

        // POST: api/Ruta
        [HttpPost]
        public async Task<ActionResult> Post([FromBody] Ruta model)
        {
            if (ModelState.IsValid) return BadRequest();

            await _ruta.save(model);

            return Ok(model);
        }

        // PUT: api/Ruta/5
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(int id, [FromBody] Ruta model)
        {
            if (id != model.RutaId) return BadRequest();

            try
            {
                await _ruta.Edit(model);
            }
            catch (Exception ex)
            {
                if (Exists(id)) return BadRequest();
                throw ex;
            }
            return Ok(model);
        }

        private bool Exists(int id) => _ruta.Exists(id);

    }
}