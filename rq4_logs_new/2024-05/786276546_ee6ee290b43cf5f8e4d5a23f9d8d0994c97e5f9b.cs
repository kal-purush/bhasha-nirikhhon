using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Azure;
using Azure.AI.TextAnalytics;

namespace MyWebAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TextAnalyticsController : ControllerBase
    {
        [HttpPost]
        public async Task<ActionResult<Dictionary<string, string>>> AnalyzeText([FromBody] string sentence)
        {
            var client = new TextAnalyticsClient(new Uri("https://mrw-ai-dev-42.openai.azure.com/"), new AzureKeyCredential("72fe1b0d6d8e4ff5ab04619acd50a8ec"));

            var response = await client.RecognizeEntitiesAsync(sentence);

            var dossier = new Dictionary<string, string>();

            foreach (var entity in response.Value)
            {
                dossier[entity.Category.ToString()] = entity.Text;
            }

            return Ok(dossier);
        }
    }
}