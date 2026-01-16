using System.Net.Mime;
using backend_carhelp.Workshop_management.Domain.Model.Queries;
using backend_carhelp.Workshop_management.Domain.Services;
using backend_carhelp.Workshop_management.Interfaces.REST.Resources;
using backend_carhelp.Workshop_management.Interfaces.REST.Transform;
using Microsoft.AspNetCore.Mvc;

namespace backend_carhelp.Workshop_management.Interfaces.REST;

[ApiController]
[Route("/api/v1/[controller]")]
[Produces(MediaTypeNames.Application.Json)]
public class ProductController(IProductCommandService productCommandService, IProductQueryService productQueryService): ControllerBase
{
    [HttpGet("{id:int}")]
    public async Task<IActionResult> GetProductById(int id)
    {
        var getProductByIdQuery = new GetProductByIdQuery(id);
        var product = await productQueryService.Handle(getProductByIdQuery);
        if(product == null) {
            return NotFound();
        }
        var productResource = ProductResourceFromEntityAssembler.ToResourceFromEntity(product);
        return Ok(productResource);
    }

    [HttpPost]
    public async Task<IActionResult> CreateProduct(CreateProductResource resource)
    {
        var createProductCommand = CreateProductCommandFromResourceAssembler.ToCommandFromResource(resource);
        var product = await productCommandService.Handle(createProductCommand);
        if (product == null)
        {
            return BadRequest();
        }
        var productResource = ProductResourceFromEntityAssembler.ToResourceFromEntity(product);
        return CreatedAtAction(nameof(GetProductById), new { id = productResource.Id }, productResource);
    }
}