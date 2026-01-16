using LoginInformation.Models;
using LoginInformation.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace LoginInformation.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class RegisterController : ControllerBase
    {
        private readonly ILogger<RegisterController> _logger;

        public RegisterController(ILogger<RegisterController> logger)
        {
            _logger = logger;
        }

        [HttpPost]
        [Route("/new-account")]
        public string NewAccount([FromBody] RegisterRequestModel user)
        {
            return "todo bien o el fallo concreto (existe el mail, algo ha fallado etc)";
        }
        [HttpPost]
        [Route("/delete-account")]
        public bool DeleteAccount([FromBody] SoloIdModel userId)
        {
            return true;
        }
    }
}