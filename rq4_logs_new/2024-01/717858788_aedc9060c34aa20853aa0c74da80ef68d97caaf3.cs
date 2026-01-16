using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using QuizzerApp.Application.Features.Commands.Question;

namespace QuizzerApp.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class QuestionController : ControllerBase
{
    private readonly ISender _sender;

    public QuestionController(ISender sender)
    {
        _sender = sender;
    }


    [HttpPost]
    public async Task<ActionResult> CreateQuestion(CreateQuestionCommand command)
    {

        var res = await _sender.Send(command);

        if (res)
            return Ok();

        throw new Exception();


    }
}