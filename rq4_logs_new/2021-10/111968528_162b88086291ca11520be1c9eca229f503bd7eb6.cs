using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

using System.Collections.Generic;

using DiceRollerUtils;
using System.IO;
using System.Collections.Specialized;
using System.Web;

namespace SlackDiceRoller
{
    public static class Rolldice
    {
        [FunctionName("rolldice")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            NameValueCollection coll = HttpUtility.ParseQueryString(requestBody);

            var diceRoller = new DiceRoller();
            List<string> diceRolls = diceRoller.CalculateRoll(coll["text"]);

            var myObj = new
            {
                response_type = "in_channel",
                text = $"<@" + coll["user_id"] + $"> roll results:",
                attachments = new object[] {
                    new {
                        text = string.Join("\n", diceRolls),
                        color = "#36a64f",
                        mrkdwn = true
                    }
                }
            };

            return new OkObjectResult(myObj);
        }
    }
}