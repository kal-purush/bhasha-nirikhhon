using Microsoft.AspNetCore.Mvc;
using QuizSite.Domain.Database;
using Microsoft.EntityFrameworkCore;
using System.Threading.Tasks;
using QuizSite.Domain;
using MediatR;
using System.Threading;
using QuizSite.Domain.Commands;
using QuizSite.Contracts.Database;

namespace QuizSite.Api.Controllers;

public class ResultController : ControllerBase
{
    private readonly IMediator _mediator;

    public ResultController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpPut("submit-result")]
    public async Task<IActionResult> SubmitQuizResultToDb([FromBody]Result quizResult, CancellationToken cancellationToken)
    {
        var command = new SubmitQuizResultCommand
        {
            Category = quizResult.QuizCategory,
            Score = quizResult.Score,
            Username = quizResult.Username
        };
        var result = await _mediator.Send(command, cancellationToken);
        return Ok(result.Id);
    }

    [HttpGet("see-results")]
    public async Task<IActionResult> GetUserResults([FromQuery]string username, CancellationToken cancellationToken) 
    {
        var command = new GetUserResultsCommand
        {
            Username = username
        };
        var result = await _mediator.Send(command, cancellationToken);
        return Ok(result);
    }
}