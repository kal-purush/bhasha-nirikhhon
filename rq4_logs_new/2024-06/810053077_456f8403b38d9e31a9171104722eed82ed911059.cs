using System.Net.Mime;
using backend_carhelp.Workshop_management.Domain.Model.Queries;
using backend_carhelp.Workshop_management.Domain.Repositories;
using backend_carhelp.Workshop_management.Domain.Services;
using backend_carhelp.Workshop_management.Interfaces.REST.Resources;
using backend_carhelp.Workshop_management.Interfaces.REST.Transform;
using Microsoft.AspNetCore.Mvc;

namespace backend_carhelp.Workshop_management.Interfaces.REST;

[ApiController]
[Route("/api/v1/[controller]")]
[Produces(MediaTypeNames.Application.Json)]
public class AdvertasingController(IAdvertasingCommandService advertasingCommandService, IAdvertasingQueryService advertasingQueryService): ControllerBase
{
    
    [HttpGet]
    public async Task<IActionResult> GetAllAdvertasing()
    {
        var getAllAdvertasingQuery = new GetAllAdvertasingQuery();
        var advertasings = await advertasingQueryService.Handle(getAllAdvertasingQuery);
        var advertasingResource = advertasings.Select(AdvertasingResourceFromEntityAssembler.ToResourceFromEntity);
        return Ok(advertasingResource);
    }
    
    [HttpGet("{id:int}")]
    public async Task<IActionResult> GetAdvertasingById(int id)
    {
        var getAdvertasingByIdQuery = new GetAdvertasingByIdQuery(id);
        var advertasing = await advertasingQueryService.Handle(getAdvertasingByIdQuery);
        if(advertasing == null) {
            return NotFound();
        }
        var advertasingResource = AdvertasingResourceFromEntityAssembler.ToResourceFromEntity(advertasing);
        return Ok(advertasingResource);
    }

    [HttpPost]
    public async Task<IActionResult> CreateAdvertasing(CreateAdvertasingResource resource)
    {
        var createAdvertasingCommand = CreateAdvertasingCommandFromResourceAssembler.ToCommandFromResource(resource);
        var advertasing = await advertasingCommandService.Handle(createAdvertasingCommand);
        if (advertasing == null)
        {
            return BadRequest();
        }
        var advertasingResource = AdvertasingResourceFromEntityAssembler.ToResourceFromEntity(advertasing);
        return CreatedAtAction(nameof(GetAdvertasingById), new { id = advertasingResource.Id }, advertasingResource);
    }
}