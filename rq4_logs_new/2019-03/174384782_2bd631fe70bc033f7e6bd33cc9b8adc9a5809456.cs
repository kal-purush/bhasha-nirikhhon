using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace TunesAPI.Controllers
{
    [Route("tunes")]
    [ApiController]
    public class TunesController : ControllerBase
    {
        List<Tunes> bangers = new List<Tunes>() {
                new Tunes { Id = 1, Artist = "Coldplay", Song = "Yellow", Album = "Viva la Vida" },
                 new Tunes { Id = 2, Artist = "Johnny Cash", Song = "The Ring of Fire", Album = "Ring of Fire" },
                 new Tunes { Id = 3, Artist = "Coldplay", Song = "Charlie Brown", Album = "X & Y" }
            };
        // GET api/values
        [HttpGet("all")]
        public IEnumerable<Tunes> GetAll()
        {
            return bangers;
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public ActionResult<string> Get(int id)
        {
            return "value";
        }

        // POST api/values
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}