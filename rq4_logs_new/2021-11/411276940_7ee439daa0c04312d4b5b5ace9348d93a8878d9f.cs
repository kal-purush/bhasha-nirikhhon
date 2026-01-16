using System.Net.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using WebApiExample.Models;

namespace WebApiExample.Controllers
{
    [ApiController]
    [Route("/api/[controller]")]
    public class AddTracksController : ControllerBase
    {
        private readonly ApplicationDbContext _context;
        private static readonly HttpClient client = new HttpClient();

        public AddTracksController(ApplicationDbContext context)
        {
            _context = context;
        }

        [HttpGet("{query}")]
        public string GetNewTracks(string query)
        {
            var url = "https://api.deezer.com/search?q=\"" + query + "\"";
            var jsonStr = client.GetStringAsync(url).Result;
            var js = JsonConvert.DeserializeObject<dynamic>(jsonStr);
            return JsonConvert.SerializeObject(js.data);
        }
    }
}