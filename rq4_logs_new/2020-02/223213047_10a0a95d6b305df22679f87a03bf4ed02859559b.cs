using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace DFC.App.MatchSkills.Controllers
{
    public class ApiDefinitionController : Controller
    {
        [HttpGet]
        [Route("/apiDefinition")]
        public IActionResult Index()
        {
            var apiDefinition = System.IO.File.ReadAllText(@"Docs\OccupationSearchAuto.json");
            return this.Ok(apiDefinition);
        }
    }
}