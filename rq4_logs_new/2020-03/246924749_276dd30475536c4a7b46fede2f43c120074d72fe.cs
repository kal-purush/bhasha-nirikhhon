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
    public class CargoController : ControllerBase
    {
        private readonly ICargo _cargo;
        public CargoController(ICargo cargo)
        {
            _cargo = cargo;
        }

        [Route("save")]
        [HttpPost]
        public async Task<ActionResult> Post([FromBody] Cargo model)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest();
            }
            await _cargo.save(model);

            return Ok(model);
        }

        [Route("Edit")]
        [HttpPut]
        public async Task<ActionResult> Put([FromBody] Cargo model, int id)
        {
            if(id != model.IdCargo)
            {
                return BadRequest();
            }
            try
            {
                 await _cargo.Edit(model);
            }
            catch (Exception ex)
            {
                if (!Exists(id))
                {
                    return BadRequest();
                }
                else
                {
                    throw ex;
                }
            }
            return Ok(model);
        }

        public bool Exists(int id)
        {
            return _cargo.Exists(id);
        }
    }
}