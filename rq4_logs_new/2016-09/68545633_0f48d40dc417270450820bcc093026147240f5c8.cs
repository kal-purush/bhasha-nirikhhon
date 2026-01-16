using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using SimpleObjectValidation.Models;
using SimpleObjectValidation.Filters;

namespace SimpleObjectValidation.Controllers
{
    [Route("api/[controller]")]
    public class SomeController : Controller
    {
        [HttpPost]
        [ValidateModelState]
        public async Task<IActionResult> SomePostMethod([FromBody] SomeModel someModel)
        {
           //Do something with someModel

            return Ok();
        }
    }
}