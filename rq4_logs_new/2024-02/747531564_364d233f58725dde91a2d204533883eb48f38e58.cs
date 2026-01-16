using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Online_Survey.DTOs.Survey;
using Online_Survey.Services;
using System.Threading.Tasks;
using Online_Survey.DTOs.Account;

namespace Online_Survey.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EmailController : ControllerBase
    {
        private readonly EmailService _emailService;

        public EmailController(EmailService emailService)
        {
            _emailService = emailService;
        }

        [HttpPost("send")]
        public async Task<IActionResult> SendEmails([FromBody] EmailSendRequestDto request)
        {
            if (request == null || request.Recipients == null || request.Recipients.Count == 0)
            {
                return BadRequest(new { message = "Invalid request. Recipients list is empty." });
            }

            bool allEmailsSent = true;

            foreach (var recipient in request.Recipients)
            {
                // Assuming recipient is already a string, no need to convert
                var emailSendDto = new EmailSendDto(recipient, request.Subject, request.Body);

                bool result = await _emailService.SendEmailAsync(emailSendDto);

                if (!result)
                {
                    allEmailsSent = false;
                    // Log or handle failure if needed
                }
            }

            if (allEmailsSent)
            {
                return Ok(new { message = "Emails sent successfully." });
            }
            else
            {
                return StatusCode(500, new { message = "Failed to send emails." });
            }
        }
    }
}