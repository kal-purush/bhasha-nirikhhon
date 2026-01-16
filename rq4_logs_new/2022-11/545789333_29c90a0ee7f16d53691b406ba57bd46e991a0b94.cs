using LogicaNegocio.Dominio;
using LogicaNegocio.InterfacesRepositorios;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace WebApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PaisesController : ControllerBase

    {
        public IRepositorioPaises RepoPaises { get; set; }

        public PaisesController(IRepositorioPaises repoPaises)
        {
            RepoPaises = repoPaises;
        }
        // GET: api/<PaisesController>
        [HttpGet]
        public IActionResult Get()
        {
            try
            {
                IEnumerable<Pais> paises = RepoPaises.FindAll();
                if (paises.Count() == 0) return NotFound();
                return Ok(paises);
            }
            catch
            {
                return StatusCode(500);
            }
           
        }

        // GET api/<PaisesController>/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            return "value";
        }

        // POST api/<PaisesController>
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        // PUT api/<PaisesController>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/<PaisesController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}