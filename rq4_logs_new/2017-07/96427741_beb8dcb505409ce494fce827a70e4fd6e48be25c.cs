using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;

namespace Lambda1.Controllers
{
    [Route("api/[controller]")]
    public class RequestController : Controller
    {
        [HttpGet]
        public IActionResult Get()
        {
            return Ok(new Dictionary<string, object>
            {
                { nameof(Request.Method), Request.Method },
                { nameof(Request.Scheme), Request.Scheme },
                { nameof(Request.Host), Request.Host },
                { nameof(Request.Path), Request.Path },
                { nameof(Request.PathBase), Request.PathBase },
                { nameof(Request.Query), Request.Query },
                { nameof(Request.Headers), Request.Headers },
            });
        }
    }
}