using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using expertlux.Services;
using Microsoft.AspNet.Mvc;
using System.Net.Http;
using Microsoft.Extensions.Configuration;

// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace expertlux.Controllers
{
    [Route("api/[controller]")]
    public class MailController : Controller
    {
        IEmailSender _emailSender;
        IConfigurationRoot _configuration;
        
        public MailController(IEmailSender emailSender, IConfigurationRoot configuration){
            _emailSender = emailSender;
            _configuration = configuration;
        }
        
        // POST api/values
        [HttpPost]
        public async Task<IActionResult> Post(CrierRequest request)
        {
            var from = _configuration["mailgun:from"];
            var responce = await _emailSender.SendEmailAsync(from, "Кричалка", request.Name + " " + request.Phone);
            var body = await responce.Content.ReadAsStringAsync();
            return Ok(body);
        }
    }
    
    public class CrierRequest
    {
        public string Name { get; set; }
        public string Phone { get; set; }
        public string MyProperty { get; set; }
    }
    
}