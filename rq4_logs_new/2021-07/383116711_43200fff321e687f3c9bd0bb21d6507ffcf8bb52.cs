using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.StaticFiles;
using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace AnagramSolver.WebApp.Controllers
{
    [ApiController]
    public class VocabularyController : Controller
    {
        private readonly string _filePath = AppDomain.CurrentDomain.SetupInformation.ApplicationBase;
        [HttpGet]
        [Route("api/getvocabulary")]
        public async Task<ActionResult> GetVocabulary()
        {
            var filePath = $"Data/zodynas.txt";

            var bytes = await System.IO.File.ReadAllBytesAsync(filePath);
            return File(bytes, "text/plain", Path.GetFileName(filePath));
        }
    }
}