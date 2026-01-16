using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.CodeAnalysis.CSharp.Scripting;

namespace WebApplication.Script.Controllers
{
    [Route("api/[controller]")]
    public class ScriptController : Controller
    {
        private ILogger _logger { get; set; }

        public ScriptController(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ScriptController>();
        }
        [HttpGet]
        public IActionResult Get()
        {
            //_logger.LogInformation("Get--------");
            var result = GetAsync();
            return Ok(result);
        }
        private int GetAsync()
        {
            var result = CSharpScript.EvaluateAsync<int>(@"
var x = 1;
var y = 2;
x + y
").GetAwaiter().GetResult();
            _logger.LogInformation($"{result}--------GetAsync");
            return result;
        }
    }
}