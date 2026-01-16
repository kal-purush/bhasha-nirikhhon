using InventoryManagement.Application.Features.Products.Commands.CreateProduct;
using InventoryManagement.Application.Features.Products.Queries.GetProductsList;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace InventoryManagement.API.Controllers;

[Route("api/[controller]")]
[ApiController]
public class ProductsController : ControllerBase
{
    private readonly IMediator _mediator;

    public ProductsController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpPost("AddProduct")]
    public async Task<ActionResult<CreateProductCommandResponse>> Create([FromBody] CreateProductCommand createProductCommand)
    {
        var response = await _mediator.Send(createProductCommand);
        return Ok(response);
    }

    [HttpGet("all", Name = "GetAllProducts")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult<List<ProductListVM>>> GetAllProducts()
    {
        var dtos = await _mediator.Send(new GetProductsListQuery());
        return Ok(dtos);
    }
}
