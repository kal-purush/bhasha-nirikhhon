using System;
using System.Collections.Generic;
using System.IO;
using IOFile = System.IO.File;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace EventCatalogAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PicController : ControllerBase
    {
        private readonly IHostingEnvironment _env;
        public PicController(IHostingEnvironment env)
        {
            _env = env;
        }
        //Getting Image from wwwroot/pics
        [HttpGet("{id}")]
        public IActionResult GetImage(int id)
        {
            var webroot = _env.WebRootPath;
            var path = Path.Combine($"{webroot}/Pics/", $"Event{id}.jpg");
            var buffer = IOFile.ReadAllBytes(path);
            return File(buffer, "image/jpeg");
        }
    }
}