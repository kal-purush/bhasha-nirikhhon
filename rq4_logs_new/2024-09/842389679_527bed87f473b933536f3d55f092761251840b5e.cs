using Application.StudentProgressTracker.Queries;
using Application.Tasks.Queries;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers;

[ApiController]
[Route("api/StudentProgress")]
public class StudentProgressController(IMediator mediator) : ControllerBase
{
    private readonly IMediator _mediator = mediator;

    [HttpGet("recent-tasks/{studentId}")]
    public async Task<IActionResult> recentTasks(string studentId)
    {
        //this is on purpose, no need to create a new query
        GetAssignedTasksQuery query = new() { StudentId = studentId };

        return Ok(await _mediator.Send(query));
    }

    [HttpPost("allTasks")]
    public async Task<IActionResult> GetModuleCompletionStatus(GetStudentTasksQuery query)
    {
        return Ok(await _mediator.Send(query));
    }

    //[HttpGet("completionStatus/{studentId}")]
    //public async Task<IActionResult> GetModuleCompletionStatus(string studentId)
    //{
    //    return Ok(await _mediator.Send());
    //}


}