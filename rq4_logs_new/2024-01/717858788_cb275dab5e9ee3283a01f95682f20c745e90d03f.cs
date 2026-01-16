using System.Net;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Identity.Client;
using QuizzerApp.Application.Features.Commands.User.CreateUser;

namespace QuizzerApp.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class UserController : ControllerBase
{
    private readonly ISender _sender;

    public UserController(ISender sender)
    {
        _sender = sender;
    }

    [HttpPost]
    public async Task<IActionResult> Register(CreateUserCommand command)
    {
        var res = await _sender.Send(command);

        if (res)
            return StatusCode(201);

        throw new Exception();
    }

}