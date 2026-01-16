using Microsoft.AspNetCore.Mvc;
using System.IO;
using Newtonsoft.Json;

namespace MonthlyMote.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MonthController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<MonthController> _logger;

        public MonthController(ILogger<MonthController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public ActionResult<Dictionary<string, string>> GetAll()
        {
            using (StreamReader r = new StreamReader("Data/months.json"))
            {
                string json = r.ReadToEnd();
                Dictionary<string, string> months = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
                return Ok(months);
            }

        }

        [HttpGet("{month}")]
        [ProducesResponseType(200)]
        [ProducesResponseType(404)]
        public ActionResult<string> GetMonth(string month)
        {
            using (StreamReader r = new StreamReader("Data/months.json"))
            {
                string json = r.ReadToEnd();
                Dictionary<string, string> months = JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
                if (months.ContainsKey(month.ToLower()))
                {
                    return Ok(months[month.ToLower()]);
                }
                return NotFound($"{month} was not found. Did you write the full name with no typos?");
            }

        }
    }
}