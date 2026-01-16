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
    public class MaterialController : ControllerBase
    {
        private readonly IMaterial _material;

        public MaterialController(IMaterial material)
        {
            _material = material;
        }

        [HttpPost]
        [Route("save")]
        public async Task<ActionResult> Post([FromBody] Material model)
        {
            if (ModelState.IsValid)
            {
                return BadRequest();
            }
            await _material.save(model);

            return Ok(model);
        }

        [HttpPut("{id}")]
        public async Task<ActionResult> Put([FromBody] Material model, int id)
        {
            if(id != model.MaterialId)
            {
                return BadRequest();
            }

            try
            {
                await _material.Edit(model);
            }
            catch (Exception ex)
            {
                if (Extist(id)) return BadRequest();
                
                throw ex;
            }

            return Ok(model);
        }

        bool Extist(int id) => _material.Exists(id);
     
    }
}