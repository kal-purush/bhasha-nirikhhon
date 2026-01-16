using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using examen_ba.DataAccess;
using examen_ba.Models;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace examen_ba.Controllers
{
    [Route("api/[controller]")]
    public class PersonasController : Controller
    {
        public PersonasController(PersonasRepository repo)
        {
            _repo = repo;
        }
        private PersonasRepository _repo;
        // GET: api/<controller>
        [HttpGet]
        public ActionResult<List<Persona>> Get()
        {
            return _repo.GetPersonas();
        }

        // GET api/<controller>/5
        [HttpGet("{id}")]
        public ActionResult<Persona> Get(long id)
        {
            var result = _repo.GetByDni(id);
           

            if (result != null)
                return result;
            else
                return NotFound();

        }

        // POST api/<controller>
        [HttpPost]
        public void Post([FromBody]string value)
        {
        }

        // PUT api/<controller>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/<controller>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}