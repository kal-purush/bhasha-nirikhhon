using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using TG.Backend.Features.Offer.CreateOffer;
using TG.Backend.Features.Offer.GetOffers;
using TG.Backend.Features.Vehicle.GetVehicles;
using TG.Backend.Models.Offer;

namespace TG.Backend.Controllers;

[ApiController]
[Route("[controller]")]
[Authorize(AuthenticationSchemes = "Bearer")]
public class OfferController : ControllerBase
{
    private readonly ISender _sender;

    public OfferController(ISender sender)
    {
        _sender = sender;
    }

    [HttpGet]
    public async Task<IActionResult> GetOffers()
    {
        var getOffersResponse = await _sender.Send(new GetOffersQuery());

        if (getOffersResponse is { IsSuccess: true, StatusCode: HttpStatusCode.OK })
            return Ok(getOffersResponse.Offers);

        return StatusCode(StatusCodes.Status503ServiceUnavailable);
    }

    [HttpPost]
    public async Task<IActionResult> CreateOffer([FromBody]CreateOfferDTO createOfferDto)
    {
        var createOfferResponse = await _sender.Send(new CreateOfferCommand(createOfferDto));

        if (createOfferResponse is { IsSuccess: true, StatusCode: HttpStatusCode.NoContent })
            return NoContent();

        return StatusCode(StatusCodes.Status503ServiceUnavailable);
    }
}