using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using EPSS.Models;
using EPSS.Repositories;

namespace EPSS.Controllers
{
    [Route("api/[controller]")]
    public class InteresadosEventosController : Controller
    {
        private IInteresadosEventosRepository _repo;
        
        public InteresadosEventosController(IInteresadosEventosRepository repo)
        {
            this._repo = repo;
        }


        [HttpGet]
        public IEnumerable<InteresadosEventos> GetAll()
        {
            return _repo.GetAll();
        }

        [HttpGet("{id}", Name = "GetInteresadosEventos")]
        public IActionResult GetById(int id)
        {
            var item = _repo.Find(id);
            if (item == null)
            {
                return NotFound();
            }
            return new ObjectResult(item);
        }

        [HttpPost]
        public IActionResult Create([FromBody] InteresadosEventos item)
        {
            if (item == null)
            {
                return BadRequest();
            }
            _repo.Add(item);
            return CreatedAtRoute("GetInteresadosEventos", new { controller = "InteresadosEventos", InteresadoEventoId = item.Id }, item);
        }

        [HttpDelete("{id}")]
        public void Delete(int id)
        {
            _repo.Remove(id);
        }
    }
}