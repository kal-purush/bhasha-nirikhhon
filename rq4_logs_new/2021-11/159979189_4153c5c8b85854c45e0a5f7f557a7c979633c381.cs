using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DelayTaskKeyspaceDemo.Controllers
{

    //https://www.c-sharpcorner.com/article/using-redis-to-done-delay-execution-in-asp-net-core/
    [Route("api/[controller]")]
    [ApiController]
    public class TaskController : ControllerBase
    {
        private readonly ITaskServices _svc;
        public TaskController(ITaskServices svc)
        {
            _svc = svc;
        }

        [HttpGet]
        public async Task<string> Get()
        {
            await _svc.DoTaskAsync();
            System.Console.WriteLine($"Dealay tasks have been scheduled at {DateTime.Now.ToString("yyyy - MM - dd HH: mm:ss")}");
            return "done";
        }
    }
}