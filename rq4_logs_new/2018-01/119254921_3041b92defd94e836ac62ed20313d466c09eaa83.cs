using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CompTech.Ict.Sample.Data;
using CompTech.Ict.Sample.Models;
using Microsoft.AspNetCore.Mvc;

namespace CompTech.Ict.Sample.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController : Controller
    {
        private readonly ApplicationContext _context;

        // Context will be passed via dependency injection
        public ValuesController(ApplicationContext context)
        {
            _context = context;
        }

        // GET api/values
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return _context.Values.Select(entity => entity.Value);
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public IActionResult Get(int id)
        {
            var entity = _context.Values.FirstOrDefault(x => x.Id == id);
            if (entity == null)
                return BadRequest("Value does not exist");
            return Ok(entity.Value);
        }

        // POST api/values
        [HttpPost]
        public IActionResult Post([FromBody]string value)
        {
            if (value == null)
                return BadRequest("Value must be provided");
            var entity = new ValueModel()
            {
                Value = value
            };
            _context.Values.Add(entity);
            _context.SaveChanges();
            return Ok(entity.Id);
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public IActionResult Put(int id, [FromBody]string value)
        {
            if (value == null)
                return BadRequest("Value must be provided");

            var entity = _context.Values.FirstOrDefault(x => x.Id == id);
            if (entity == null)
                return BadRequest("Value does not exist");

            entity.Value = value;
            _context.Update(entity);
            _context.SaveChanges();
            return Ok();
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public IActionResult Delete(int id)
        {
            var entity = _context.Values.FirstOrDefault(x => x.Id == id);
            if (entity == null)
                return BadRequest("Value does not exist");
            
            _context.Remove(entity);
            _context.SaveChanges();
            return Ok();
        }
    }
}