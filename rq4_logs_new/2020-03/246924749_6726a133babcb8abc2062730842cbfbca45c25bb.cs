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
    public class DetalleCotizacionController : ControllerBase
    {
        private readonly IDetalleCotizacion _detalleCotizacion;
        // POST: api/DetalleCotizacion
        [HttpPost]
        [Route("save")]
        public async Task<ActionResult> Post([FromBody] DetalleCotizacion model)
        {
            if (!ModelState.IsValid) return BadRequest();

            await _detalleCotizacion.save(model);

            return Ok(model);
        }

        // PUT: api/DetalleCotizacion/5
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(int id, [FromBody] DetalleCotizacion model)
        {
            if (id != model.CotizacionId) return BadRequest();

            try
            {
                await _detalleCotizacion.Edit(model);
            }
            catch (Exception ex)
            {
                if(!Exists(id)) return BadRequest();
                throw ex;
            }
            return Ok(model);
        }

        public bool Exists(int id) => _detalleCotizacion.Exists(id);
       
    }
}