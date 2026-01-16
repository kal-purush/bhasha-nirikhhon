using AnagramSolver.BusinessLogic.Classes;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace AnagramSolver.WebApp.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class GetDictionaryController : ControllerBase
    {
        private readonly ILogger<GetDictionaryController> _logger;
        private readonly string _filePath = AppDomain.CurrentDomain.SetupInformation.ApplicationBase + "Data/zodynas.txt";
        private readonly WordRepository _wordRepository;
        public GetDictionaryController(ILogger<GetDictionaryController> logger)
        {
            _logger = logger;
            _wordRepository = new WordRepository(_filePath, 4);

        }
        [HttpGet("[action]/{id}")]
        public HttpResponseMessage Download()
        {
            var data = "YourDataHere";
            byte[] bytes = System.Text.Encoding.UTF8.GetBytes(data);
            var output = new FileContentResult(bytes, "application/octet-stream");
            output.FileDownloadName = "download.txt";

            return output;
        }
    }
}