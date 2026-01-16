using MediatR;
using Microsoft.AspNetCore.Mvc;
using QuizzerApp.Application.Features.Commands.Answer.CreateAnswer;

namespace QuizzerApp.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AnswerController : ControllerBase
{
    private readonly ISender _sender;

    public AnswerController(ISender sender)
    {
        _sender = sender;
    }

    [HttpPost]
    public async Task<IActionResult> CreateAnswer(CreateAnswerCommand command)
    {
        var res = await _sender.Send(command);

        if (res)
            return StatusCode(201);

        throw new Exception();
    }

}