using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using TG.Backend.Features.Vehicle.CreateVehicle;
using TG.Backend.Features.Vehicle.GetVehicles;

namespace TG.Backend.Controllers;

[ApiController]
[Route("[controller]")]
[Authorize(AuthenticationSchemes = "Bearer")]
public class VehicleController : ControllerBase
{
    private readonly ISender _sender;

    public VehicleController(ISender sender)
    {
        _sender = sender;
    }

    [HttpGet]
    public async Task<IActionResult> GetVehicles()
    {
        var vehicleResponse = await _sender.Send(new GetVehiclesQuery());

        if (vehicleResponse is { IsSuccess: true, StatusCode: HttpStatusCode.OK })
            return Ok(vehicleResponse.Vehicles);

        return StatusCode(503);
    }

    [HttpPost]
    public async Task<IActionResult> CreateVehicle([FromBody]VehicleDTO vehicleDto)
    {
        var createVehicleResponse = await _sender.Send(new CreateVehicleCommand(vehicleDto));

        if (createVehicleResponse is { IsSuccess: true, StatusCode: HttpStatusCode.NoContent })
            return NoContent();

        return StatusCode(503);
    }
}