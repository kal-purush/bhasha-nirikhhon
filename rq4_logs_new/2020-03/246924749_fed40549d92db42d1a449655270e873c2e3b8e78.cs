using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WaterSolutionAPI.Interfaces;
using WaterSolutionAPI.ModelDTO;
using WaterSolutionAPI.Models;

namespace WaterSolutionAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ClienteController : ControllerBase
    {
        private readonly ICliente _cliente;

        public ClienteController(ICliente cliente)
        {
            _cliente = cliente;
        }

        [Route("MostrarClientes")]
        [HttpGet]
        public async Task<List<ClienteDTO>> MostrarClientes(int id)
        {
            return await _cliente.MostrarCliente(id);
        }
      
        // POST: api/Cliente
        [Route("Save")]
        [HttpPost]
        public async Task<ActionResult> Post([FromBody] Cliente model)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest();
            }

            await _cliente.Save(model);

            return Ok(model);
        }

        // PUT: api/Cliente/5
        [Route("Edit")]
        [HttpPut("{id}")]
        public async Task<ActionResult> Put(int id, [FromBody] Cliente model)
        {
            if(id != model.PersonaId)
            {
                return BadRequest();
            }
            try
            {
                await _cliente.Edit(model);
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

        bool Exists(int id)
        {
            return _cliente.Exists(id);
        }
    }
}