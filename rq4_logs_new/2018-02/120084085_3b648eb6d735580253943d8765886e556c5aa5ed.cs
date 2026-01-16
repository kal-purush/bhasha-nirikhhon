using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WebApplication1.Model;

namespace WebApplication1.Controllers
{
    [Produces("application/json")]
    [Route("api/DataTest")]
    public class DataTestController : Controller
    {
        [HttpGet("GetNames")]
        public IActionResult GetNames()
        {
            var list = new List<Hero>
            {
                new Hero{HeroName="Iron Man", Comics = "Marvel"},
                new Hero{HeroName="Superman", Comics = "DC"},
                new Hero{HeroName="Batman", Comics = "DC"},
                new Hero{HeroName="Captain America", Comics = "Marvel"},
                new Hero{HeroName="Thor", Comics = "Marvel"},
                new Hero{HeroName="Aqua Man", Comics = "DC"},

            };
            var ob = new JsonResult(list);
            return Ok(ob.Value);
        }
    }
}