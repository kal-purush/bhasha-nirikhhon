using System.Collections.Generic;
using Buldo.Ngb.Bot.EnginesManagement;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Buldo.Ngb.Web.Controllers
{
    [Route("api/[controller]")]
    public class EnginesController : Controller
    {
        // GET: api/values
        [HttpGet]
        public IEnumerable<EngineInfo> Get()
        {
            return new EngineInfo[] { };
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            return "value";
        }

        // POST api/values
        [HttpPost]
        public void Post([FromBody]EngineInfo value)
        {
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]EngineInfo value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}