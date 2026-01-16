using Application.CQRS.Account.Static;
using Application.CQRS.Ratings.Comands.AddRating;
using Application.CQRS.Ratings.Comands.UpdateRating;
using Application.CQRS.Ratings.Dtos;
using Application.CQRS.Ratings.Queries;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers.Areas.Ratings;
[Route("api/rating")]
public class RatingController:BaseController
{
    [Authorize(Roles = UserRoles.User)]
    [HttpPost("add-ratting")]
    public async Task<IActionResult> AddRating([FromBody] AddRatingCommand command, CancellationToken cancellationToken)
    {
        var response = await Mediator.Send(command, cancellationToken);
        return Ok(response);
    }
    
    
    
    [Authorize(Roles = UserRoles.User)]
    [HttpPut("update-ratting")]
    public async Task<IActionResult> UpdateRating([FromBody] UpdateRatingCommand command, CancellationToken cancellationToken)
    {
        var response = await Mediator.Send(command, cancellationToken);
        return Ok(response);
    }

    [Authorize(Roles = UserRoles.User)]
    [HttpGet("{PlaceId}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult<RatingDto>> DisplayRating([FromRoute] DisplayRatingQuery query,
        CancellationToken cancellationToken)
    {
        var response = await Mediator.Send(query, cancellationToken);
        return Ok(response);
    }
}