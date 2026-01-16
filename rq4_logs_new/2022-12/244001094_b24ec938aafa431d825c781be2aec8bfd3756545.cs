namespace devanewbot.Api.v0.Controllers;

using System.Threading.Tasks;
using AspNetCore.ReCaptcha;
using devanewbot.Api.v0.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using SlackDotNet;

[Route("/api/v0/signup")]
public class SignupController : Controller
{
    protected Slack Slack { get; }

    protected IReCaptchaService ReCaptchaService { get; }

    protected ReCaptchaSettings ReCaptchaSettings { get; }

    public SignupController(Slack slack,
         IReCaptchaService reCaptchaService,
         IOptions<ReCaptchaSettings> reCaptchaSettings)
    {
        Slack = slack;
        ReCaptchaService = reCaptchaService;
        ReCaptchaSettings = reCaptchaSettings.Value;
    }

    [HttpGet]
    public async Task<IActionResult> GetToken()
    {
        return Ok(new
        {
            Token = ReCaptchaSettings.SiteKey
        });
    }

    [HttpPost]
    public async Task<IActionResult> Signup([FromBody] SignupModel model)
    {
        if (!await ReCaptchaService.VerifyAsync(model.Token))
        {
            return BadRequest(new { Error = "token_verification_failed" });
        }

        var (Success, Error) = await Slack.InviteUser(model.Email);
        if (Success)
        {
            return Ok();
        }

        return BadRequest(new { Error });
    }
}