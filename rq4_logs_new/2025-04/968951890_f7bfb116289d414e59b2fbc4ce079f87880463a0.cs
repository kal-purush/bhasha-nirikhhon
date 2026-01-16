using Lucky7_Inventory_System_Application.Commands.RoleCommands;
using Lucky7_Inventory_System_Application.Queries.RoleQueries;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Lucky7_Inventory_System_API.Controllers;

[ApiController]
[Route("[controller]")]
public class RoleController : Controller
{
    private readonly IMediator _mediator;

    public RoleController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpPost("CreateRole")]
    public async Task<IActionResult> CreateRole([FromQuery] CreateRoleCommand command)
    {
        var result = await _mediator.Send(command);
        return Ok(result);
    }

    [HttpPut("UpdateRole")]
    public async Task<IActionResult> UpdateRole([FromQuery] UpdateRoleCommand command)
    {
        var result = await _mediator.Send(command);
        return Ok(result);
    }

    [HttpDelete("DeleteRole")]
    public async Task<IActionResult> DeleteRole([FromQuery] DeleteRoleCommand command)
    {
        var result = await _mediator.Send(command);
        return Ok(result);
    }

    [HttpGet("{id}")]
    public async Task<IActionResult> GetRoleById(int id)
    {
        var result = await _mediator.Send(new GetRoleByIdQuery { Id = id });
        return Ok(result);
    }

    [HttpGet("GetAllRoles")]
    public async Task<IActionResult> GetAllRoles()
    {
        var result = await _mediator.Send(new GetAllRolesQuery());
        return Ok(result);
    }
}