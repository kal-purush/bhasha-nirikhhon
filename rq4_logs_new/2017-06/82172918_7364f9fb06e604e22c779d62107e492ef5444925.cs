using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;

namespace WebApi.Controllers
{
    [Route("api/[controller]")]
    public class BuildingsController : Controller
    {
        // GET api/buildings
        [HttpGet]
        public IActionResult Get()
        {
            var buildings = new[]
            {
                new BuildingDto("Building1", true),
                new BuildingDto("building2", false)
            };
            return Ok(buildings);
        }

        // GET api/values/5
        [HttpGet("{id}")]
        public string Get(int id)
        {
            return "value";
        }

        // POST api/values
        [HttpPost]
        public void Post([FromBody]string value)
        {
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }

    internal class BuildingDto
    {
        public BuildingDto(string name, bool ownedByMy)
        {
            Name = name;
            OwnedByMy = ownedByMy;
        }

        public string Name { get; }

        public bool OwnedByMy { get; }
    }
}