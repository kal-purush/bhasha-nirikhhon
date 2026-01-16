using Microsoft.AspNetCore.Mvc;
using PetFamily.API.Controllers.Pets.Requests;
using PetFamily.API.Extensions;
using PetFamily.Application.Volunteers.Queries.GetPetsWithPagination;

namespace PetFamily.API.Controllers.Pets
{
    public class PetsController : ApplicationController
    {
        [HttpGet]
        public async Task<ActionResult> GetPetsWithPagination(
            [FromQuery] GetPetsWithPaginationRequest request,
            [FromServices] GetPetsWithPaginationHandler handler,
            CancellationToken cancellationToken)
        {
            var result = await handler.Handle(request.ToQuery(), cancellationToken);
            if (result.IsFailure)
                return result.Error.ToResponse();

            return Ok(result.Value);
        }
    }
}