using AutoMapper;
using Event_Planning_System.DTO.Mail;
using Event_Planning_System.IServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Net.Mail;

namespace Event_Planning_System.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MailController : ControllerBase
    {
        ISendEmailService Mailer;
        public MailController(ISendEmailService _m)
        {
            Mailer = _m;
        }
        [HttpPost]
        public IActionResult Send([FromBody]SendEmailDto sendemail)
        {
            if(!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            try
            {
                var response  = Mailer.SendEmail(sendemail);
                return Ok(response);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }
    }
}