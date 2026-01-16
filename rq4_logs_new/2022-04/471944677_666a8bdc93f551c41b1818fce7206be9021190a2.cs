using System.Web;
using Microsoft.AspNetCore.Mvc;
using UProgress.Contracts.Messages;
using UProgress.Service.Interfaces;
using UProgress.Service.Services;


namespace UProgress.Service.Web.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AuthController : ControllerBase
{
    private readonly AuthService _authService;
    private readonly IEmailService _emailService;
    private readonly string _frontendBaseUrl;

    public AuthController(AuthService authService, IEmailService emailService, IConfiguration configuration)
    {
        _authService = authService;
        _emailService = emailService;
        _frontendBaseUrl = configuration.GetValue<string>("FrontendBaseUrl");
    }

    [HttpPost("login")]
    public async Task<IActionResult> LoginAsync(Login message)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest(new ApiBadRequest("Логин или пароль не валидны"));
        }

        var authToken = await _authService.GetAuthToken(message.UsernameOrEmail, message.Password);
        if (authToken == null)
        {
            return BadRequest(new ApiBadRequest("Неверный логин или пароль"));
        }

        var hasUserEmailConfirmation = await _authService.HasUserEmailConfirmation(message.UsernameOrEmail);
        if (!hasUserEmailConfirmation)
        {
            return BadRequest(new ApiBadRequest("Пожалуйста, подтвердите почту перед тем как войти"));
        }

        Response.Cookies.Append("uprogress-st", authToken, new CookieOptions()
        {
            SameSite = SameSiteMode.None,
            Secure = true
        });

        return Ok();
    }

    [HttpPost("requestreset")]
    public async Task<IActionResult> RequestPasswordResetAsync(RequestPasswordReset message)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest();
        }

        var passwordResetToken = await _authService.GetPasswordResetTokenAsync(message.Email);
        if (passwordResetToken == null)
        {
            return Ok();
        }

        var encodedResetToken = HttpUtility.UrlEncode(passwordResetToken);
        var passwordResetLink =
            $"{_frontendBaseUrl}/auth/resetpassword/?token={encodedResetToken}&email={message.Email}";

        _emailService.SendEmail(message.Email, "UProgress: Восстановление пароля",
            $"<a href='{passwordResetLink}'>Восстановить пароль</a>");

        return Ok();
    }

    [HttpPost("reset")]
    public async Task<IActionResult> ResetPasswordAsync(ResetPassword message)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest();
        }

        var isPasswordResetSucceeded =
            await _authService.ResetPasswordAsync(message.Email, message.ResetToken, message.NewPassword);
        if (!isPasswordResetSucceeded)
        {
            return BadRequest();
        }

        return Ok();
    }

    [HttpPost("confirm")]
    public async Task<IActionResult> ConfirmEmailAsync(string email, string emailConfirmToken)
    {
        if (!ModelState.IsValid)
        {
            return BadRequest();
        }

        var isConfirmEmailSucceeded = await _authService.ConfirmEmailAsync(email, emailConfirmToken);
        if (!isConfirmEmailSucceeded)
        {
            return BadRequest();
        }

        return Redirect(_frontendBaseUrl);
    }
}