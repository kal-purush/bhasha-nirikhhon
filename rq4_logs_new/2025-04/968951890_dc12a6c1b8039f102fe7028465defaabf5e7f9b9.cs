using Lucky7_Inventory_System_Application.Commands.StatusCommands;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Lucky7_Inventory_System_API.Controllers;

[ApiController]
[Route("[controller]")]
public class StatusController : Controller
{
    private readonly IMediator _mediator;

    public StatusController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpPost("CreateStatus")]
    public async Task<IActionResult> CreateStatus([FromQuery]CreateStatusCommand command)
    {
        var result = await _mediator.Send(command);
        return Ok(result);
    }

    [HttpPut("UpdateStatus")]
    public async Task<IActionResult> UpdateStatus([FromQuery] UpdateStatusCommand command)
    {
        var result = await _mediator.Send(command);
        return Ok(result);
    }

    [HttpDelete("DeleteStatus")]
    public async Task<IActionResult> DeleteStatus([FromQuery] UpdateStatusCommand command)
    {
        var result = await _mediator.Send(command);
        return Ok(result);
    }
}