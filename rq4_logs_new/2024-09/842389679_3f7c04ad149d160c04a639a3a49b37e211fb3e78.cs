using Application.Admins.Commands;
using Application.Auth.Commands;

//using Application.AuthController.Queries;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;


namespace API.Controllers
{
    [ApiController]
    [Route("api/admin")]
    public class AdminAuthenticationController(IMediator mediator) : ControllerBase
    {
        private readonly IMediator _mediator = mediator;

        [HttpPost("register")]
        public async Task<IActionResult> Register(RegisterAdminCommand command)
        {
            return Ok(await _mediator.Send(command));
        }
        

        [HttpPost("login")]
        public async Task<IActionResult> Login(LoginAdminCommand command)
        {
            return Ok(await _mediator.Send(command));
        }
        
        [HttpPost("forgot-password")]
        public async Task<IActionResult> ForgetPassword(ForgotAdminPasswordCommand command)
        {
            return Ok(await _mediator.Send(command));
        }

        
        [HttpPost("reset-password")]
        public async Task<IActionResult> ResetPassword(ResetAdminPasswordCommand command)
        {
            return Ok(await _mediator.Send(command));
        }
    }
}