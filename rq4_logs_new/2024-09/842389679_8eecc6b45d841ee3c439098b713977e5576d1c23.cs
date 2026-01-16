using Application.Answers.Queries;
using Application.Students.Queries;
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Application.Auth.Commands;
using Application.Tasks.Queries;
using Application.Programmes.Commands;
using Application.Questions.Commands;
using Application.Questions.Queries;
using Application.Quizzes.Commands;


namespace API.Controllers;

[ApiController]
[Route("api/question")]
public class QuestionController(IMediator mediator) : Controller
{

    private readonly IMediator _mediator = mediator;

    [HttpPost("create")]
    public async Task<IActionResult> CreateQuestion(CreateQuestionCommand command)
    {
        return Ok(await _mediator.Send(command));
    }
    
    [HttpPost("add-options")]
    public async Task<IActionResult> AddOptions(UpdateQuestionOptionCommand command)
    {
        return Ok(await _mediator.Send(command));
    }

    [HttpGet("{questionId}/all-details")]
    public async Task<IActionResult> GetQuestionDetailsById(int questionId)
    {
        return Ok(await _mediator.Send(new GetQuestionByIdQuery() { QuestionId = questionId }));

    }
    
    [HttpGet("{questionId}/answers")]
    public async Task<IActionResult> GetAllAnswersByQuestionId(int questionId)
    {
        return Ok(await _mediator.Send(new GetAllAnswersByQuestionIdQuery() { QuestionId = questionId }));

    }
}