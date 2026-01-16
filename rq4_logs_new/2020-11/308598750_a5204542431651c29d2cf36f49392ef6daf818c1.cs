using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using SendGrid;
using SendGrid.Helpers.Mail;

namespace api.layer.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class EmailController : Controller
    {
        [HttpGet]
        public IEnumerable<WeatherForecast> Get()
        {
            Execute().Wait();
            var rng = new Random();
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = rng.Next(-20, 55)
            })
            .ToArray();
        }

        static async Task<Response> Execute()
        {
            SendGridClientOptions sendGridClientOptions = new SendGridClientOptions();
            sendGridClientOptions.ApiKey = "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";
            sendGridClientOptions.Auth = new AuthenticationHeaderValue("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");
            sendGridClientOptions.Host = "587";
            sendGridClientOptions.Version = "v3";
            sendGridClientOptions.UrlPath = "smtp-relay.sendinblue.com";

            var client = new SendGridClient(sendGridClientOptions);
            var from = new EmailAddress("pavandeep.singh@gep.com", "Pavandeep Singh");
            var to = new EmailAddress("amit.mishra@gep.com", "Amit Mishra");
            var subject = "You won Kudos";
            var plainTextContent = "for your amazing work";
            var htmlContent = "<strong> keep up the good work</strong>";
            var msg = MailHelper.CreateSingleEmail(
                from, to, subject, plainTextContent, htmlContent);

            var response = await client.SendEmailAsync(msg);
            return response;
        }
    }
}