using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WaterSolutionAPI.Interfaces;
using WaterSolutionAPI.Models;

namespace WaterSolutionAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DepartamentoController : ControllerBase
    {
        private readonly IDepartamento _departamento;

        public DepartamentoController(IDepartamento departamento)
        {
            _departamento = departamento;
        }

        // POST: api/Departamento
        [HttpPost]
        public async Task<ActionResult> Post([FromBody] Departamentos model)
        {
            if (!ModelState.IsValid) return BadRequest();

             await _departamento.save(model);

            return Ok(model); 
        }

        // PUT: api/Departamento/5
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(int id, [FromBody] Departamentos model)
        {
            if (id != model.IdDepartamento) return BadRequest();

            try
            {
                await _departamento.Edit(model);
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
            return _departamento.Exists(id);
        }
    }
}