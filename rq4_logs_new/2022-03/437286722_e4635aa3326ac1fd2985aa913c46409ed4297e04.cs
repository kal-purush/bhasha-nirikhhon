using System.Web;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Webservice.Helper;
using Webservice.Interfaces;
using Webservice.Models;
using Webservice.Services;

namespace Webservice.Controllers.v1;

[ApiVersion("1.0")]
[Route("api/v{v:apiVersion}/[controller]")]
[ApiController]
[Authorize(Roles = "Admin")]
public class TomasiController : ControllerBase
{
    private readonly EmailService _emailService;
    private readonly ILogService _logService;

    public TomasiController(IOptions<EmailSettings> options, ILogService logService)
    {
        _emailService = new EmailService(options);
        _logService = logService;
    }

    [HttpPost]
    public async Task<IActionResult> Post(Mail mail)
    {
        try
        {
            const string subject = "Kontaktformular Webseite";
            var message = EmailMessagesService.CreateContactFormMailMessage(mail);
            var checkSend =
                await _emailService.SendEmailAsync(HttpUtility.HtmlEncode(mail.ReceiverAddress), message, subject);
            if (!checkSend) return BadRequest("Email konnte nicht gesendet werden.");
            return Ok(checkSend);
        }
        catch (Exception e)
        {
            await _logService.LogAsync(new Log
            {
                Requester = User.Claims.First().Value,
                RequestMethod = "GlasblaesereiEgli -> Send Contact mail",
                RequestDate = DateTime.Now,
                LogTypeId = (int) Constantes.LogTypes.ERROR,
                ErrorMessage = e.Message,
                InnerException = e.InnerException?.ToString()
            });
            return BadRequest("Email konnte nicht gesendet werden.");
        }
    }
}