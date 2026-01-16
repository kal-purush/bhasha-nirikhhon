using Microsoft.AspNetCore.Mvc;
using PetFamily.Application.Breeds.Queries.GetBreedsBySpeciesId;

namespace PetFamily.API.Controllers.Breeds;

public class BreedsController : ApplicationController
{
    [HttpGet("/species/{id:guid}")]
    public async Task<ActionResult> AllBreedsBySpeciesId(
        [FromRoute] Guid id,
        [FromServices] GetBreedsBySpeciesIdHandler handler,
        CancellationToken cancellationToken)
    {
        var query = new GetBreedsBySpeciesIdQuery(id);
        var result = await handler.HandleAsync(query, cancellationToken);
        return Ok(result);
    }
}