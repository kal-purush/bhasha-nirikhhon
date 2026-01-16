using InventoryManagement.Application.Features.Inventories.Commands.CreateInventory;
using InventoryManagement.Application.Features.Inventories.Commands.UpdateInventory;
using InventoryManagement.Application.Features.Inventories.Queries.GetInventoriesByName;
using InventoryManagement.Application.Features.Inventories.Queries.GetInventoriesList;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace InventoryManagement.API.Controllers;

[Route("api/[controller]")]
[ApiController]
public class InventoryController : ControllerBase
{
    private readonly IMediator _mediator;

    public InventoryController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpPost(Name = "AddInventory")]
    public async Task<ActionResult<CreateInventoryCommandResponse>> Create([FromBody] CreateInventoryCommand createInventoryCommand)
    {
        var response = await _mediator.Send(createInventoryCommand);
        return Ok(response);
    }

    [HttpGet("all", Name = "GetAllInventories")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult<List<InventoryListVM>>> GetAllInventories()
    {
        var dtos = await _mediator.Send(new GetInventoriesListQuery());
        return Ok(dtos);
    }

    [HttpGet("{name}", Name = "GetInventoriesByName")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult<List<InventoryByNameVM>>> GetInventoriesByName(string name)
    {
        var dtos = await _mediator.Send(new GetInventoriesByNameQuery { Name = name });
        if (dtos.Any())
            return Ok(dtos);

        return NotFound();
    }

    [HttpPut(Name = "UpdateInventory")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesDefaultResponseType]
    public async Task<ActionResult> Update([FromBody] UpdateInventoryCommand updateInventoryCommand)
    {
        var dtos = await _mediator.Send(updateInventoryCommand);
        return Ok(dtos);
    }

}