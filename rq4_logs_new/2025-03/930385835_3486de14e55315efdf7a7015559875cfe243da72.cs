using Microsoft.AspNetCore.Mvc;
using PetFamily.API.Controllers.Species.Requests;
using PetFamily.Application.Species.Queries.GetBreedsBySpeciesId;
using PetFamily.Application.Species.Queries.GetSpeciesWithPagination;

namespace PetFamily.API.Controllers.Species;

public class SpeciesController : ApplicationController
{
    [HttpGet]
    public async Task<ActionResult> Species(
        [FromQuery] GetSpeciesWithPaginationRequest request,
        [FromServices] GetSpeciesWithPaginationHandler handler,
        CancellationToken cancellationToken)
    {
        var query = request.ToQuery();
        var result = await handler.HandlerAsync(query, cancellationToken);
        return Ok(result);
    }

    [HttpGet("{id:guid}/breeds")]
    public async Task<ActionResult> Breeds(
        [FromRoute] Guid id,
        [FromServices] GetBreedsBySpeciesIdHandler handler,
        CancellationToken cancellationToken)
    {
        var query = new GetBreedsBySpeciesIdQuery(id);
        var result = await handler.HandlerAsync(query, cancellationToken);
        return Ok(result);
    }
}