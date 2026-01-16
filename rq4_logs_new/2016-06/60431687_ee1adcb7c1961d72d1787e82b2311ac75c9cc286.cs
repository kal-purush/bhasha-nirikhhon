using System.Collections.Generic;
using System.Web.Http;

namespace OverwatchSynergy.Api
{
    [RoutePrefix("heroes")]
    public class HeroesController : ApiController
    {
        // GET api/values 
        [HttpGet, Route("")]
        public IEnumerable<string> Get()
        {
            return new string[] {   "Genji", "McCree", "Pharah", "Reaper", "Soldier: 76",
                                    "Tracer", "Bastion", "Hanzo", "Junkrat", "Mei", "Torbjörn",
                                    "Widowmaker", "D.Va", "Reinhardt", "Roadhog", "Winston",
                                    "Zarya", "Lúcio", "Mercy", "Symmetra", "Zenyatta" };
        }
    }
}