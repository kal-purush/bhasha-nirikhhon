using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using ContactSvc.Dtos;
// using MailKit.Net.Smtp;
// using MimeKit;
// using Microsoft.Identity.Client;
using System.Threading.Tasks;
//using MailKit.Security;
using Microsoft.Extensions.Configuration;
using ContactSvc.Settings;
using ContactSvc.Data;

namespace TigerStride.ContactSvc
{
    /// <summary>
    /// Implement the HttpTrigger for an Azure Function that responds to the POST of the contact form on the corporate homepage.
    /// </summary> 
    public partial class HttpTriggerContact
    {
        [Function("CheckHealth")]
        public IActionResult CheckHealth([HttpTrigger(AuthorizationLevel.Anonymous, "get")] HttpRequest req)
        {
            try
            {
                _logger.LogInformation("Health check requested.");
                return new OkObjectResult(new { status = "Healthy" });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Health check failed.");
                return new StatusCodeResult(StatusCodes.Status500InternalServerError);
            }
        }
    }
}