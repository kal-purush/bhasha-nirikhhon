using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Service.TextAnalysis.Services;

namespace Service.TextAnalysis.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class IndexController : ControllerBase
    {
        private readonly IAnalyzeSentimentService _analyzeSentimentService;

        public IndexController(IAnalyzeSentimentService analyzeSentimentService)
        {
            if (analyzeSentimentService == null) throw new ArgumentNullException(nameof(analyzeSentimentService));

            _analyzeSentimentService = analyzeSentimentService;
        }

        [HttpGet("{text}")]
        public ActionResult<string> Get(string text)
        {
            return "value";
        }
    }
}