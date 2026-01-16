using Application.Admins.Commands;
using Application.Instructors.Commands;
using Application.LearningAdmins.Commands;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;


namespace API.Controllers
{
    [ApiController]
    [Route("api/instructor")]
    public class InstructorAuthenticationController(IMediator mediator) : ControllerBase
    {
        private readonly IMediator _mediator = mediator;
        

        [HttpPost("login")]
        public async Task<IActionResult> Login(LoginInstructorCommand command)
        {
            return Ok(await _mediator.Send(command));
        }
        
        [HttpPost("forgot-password")]
        public async Task<IActionResult> ForgetPassword(ForgotInstructorPasswordCommand command)
        {
            return Ok(await _mediator.Send(command));
        }

        
        [HttpPost("reset-password")]
        public async Task<IActionResult> ResetPassword(ResetInstructorPasswordCommand command)
        {
            return Ok(await _mediator.Send(command));
        }
    }
}