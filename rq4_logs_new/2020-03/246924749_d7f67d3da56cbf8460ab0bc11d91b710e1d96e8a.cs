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
    public class CotizacioneController : ControllerBase
    {
        private readonly ICotizaciones _cotizacion;

        public CotizacioneController(ICotizaciones cotizacion)
        {
            _cotizacion = cotizacion;
        }

        [HttpPost]
        [Route("save")]
        public async Task<ActionResult> Post([FromBody] Cotizaciones model)
        {
            if (!ModelState.IsValid) return BadRequest();
      
            await _cotizacion.save(model);

            return Ok(model);
        }

        [HttpPut]
        [Route("Edit")]
        public async Task<ActionResult<Cotizaciones>> Put([FromBody] Cotizaciones model, int id)
        {
            if (id != model.CotizacionId) return BadRequest();

            try
            {
                await _cotizacion.Edit(model);
            }
            catch (Exception ex)
            {
                if (!Exists(id)) return BadRequest();
                
                throw ex;
            }

            return Ok(model);
        }

        public bool Exists(int id)
        {
            return _cotizacion.Exists(id);
        }
    }
}