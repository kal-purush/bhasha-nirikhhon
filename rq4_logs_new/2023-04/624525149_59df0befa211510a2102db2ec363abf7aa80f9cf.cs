using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace TG.Backend.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class AuthController : ControllerBase
    {
        private readonly IMediator _mediator;

        public AuthController(IMediator mediator)
        {
            _mediator = mediator;
        }

        [HttpPost("register")]
        public async Task<IActionResult> Register([FromBody] AppUserRegisterDTO appUser)
        {
            AuthResponseModel resp = await _mediator.Send(new RegisterUserCommand(appUser));

            return resp switch
            {
                { IsSuccess: false, StatusCode: HttpStatusCode.BadRequest } => BadRequest(resp.Messages),
                { IsSuccess: true, StatusCode: HttpStatusCode.OK } => Ok(resp.Messages.FirstOrDefault()),
                _ => BadRequest()
            };
        }
    }
}