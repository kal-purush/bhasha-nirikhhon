using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FirstAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DevNewcontroller : ControllerBase
    {
        [HttpGet]
        public ActionResult<string> GetDev()
        {
            return "Dev";
        }


    }
}