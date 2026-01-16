using AutoMapper;
using Catcheap.API.DTO;
using Catcheap.API.Interfaces.IRepository.ICarRepo;
using Catcheap.API.Interfaces.IService.ICarServices;
using Catcheap.API.Models.CarModels;
using Microsoft.AspNetCore.Mvc;

namespace Catcheap.API.Controllers.CarControl;

[Route("api/[controller]")]
[ApiController]
public class CarStatsController : Controller
{
    private readonly ICarRepository _carRepository;

    private readonly ICarStatsService _statsService;

    public CarStatsController(ICarRepository carRepository, ICarStatsService statsService)
    {
        _carRepository = carRepository;
        _statsService = statsService;
    }

    [HttpGet("{carId}/ChargesSince/{startDate}")]
    [ProducesResponseType(200, Type = typeof(double))]
    [ProducesResponseType(400)]
    public IActionResult GetCarChargesCount(int carId, DateTime startDate)
    {
        if (!_carRepository.CarExists(carId))
            return NotFound();

        var count = _statsService.GetChargesCountSince(carId, startDate);

        if (!ModelState.IsValid)
            return BadRequest(ModelState);

        return Ok(count);
    }

    [HttpGet("{carId}/JourneysSince/{startDate}")]
    [ProducesResponseType(200, Type = typeof(double))]
    [ProducesResponseType(400)]
    public IActionResult GetCarJourneysCount(int carId, DateTime startDate)
    {
        if (!_carRepository.CarExists(carId))
            return NotFound();

        var count = _statsService.GetJourneysCountSince(carId, startDate);

        if (!ModelState.IsValid)
            return BadRequest(ModelState);

        return Ok(count);
    }

    [HttpGet("{carId}/JourneyDistanceSince/{startDate}")]
    [ProducesResponseType(200, Type = typeof(double))]
    [ProducesResponseType(400)]
    public IActionResult GetCarJourneyDistance(int carId, DateTime startDate)
    {
        if (!_carRepository.CarExists(carId))
            return NotFound();

        var distance = _statsService.GetJourneyDistanceSince(carId, startDate);

        if (!ModelState.IsValid)
            return BadRequest(ModelState);

        return Ok(distance);
    }

    [HttpGet("{carId}/ConsumptionSince/{startDate}")]
    [ProducesResponseType(200, Type = typeof(double))]
    [ProducesResponseType(400)]
    public IActionResult GetCarConsumption(int carId, DateTime startDate)
    {
        if (!_carRepository.CarExists(carId))
            return NotFound();

        var consumption = _statsService.GetConsumptionSince(carId, startDate);

        if (!ModelState.IsValid)
            return BadRequest(ModelState);

        return Ok(consumption);
    }

    [HttpGet("{carId}/MoneySince/{startDate}")]
    [ProducesResponseType(200, Type = typeof(double))]
    [ProducesResponseType(400)]
    public IActionResult GetCarMoneySpent(int carId, DateTime startDate)
    {
        if (!_carRepository.CarExists(carId))
            return NotFound();

        var moneySpent = _statsService.GetMoneySpentSince(carId, startDate);

        if (!ModelState.IsValid)
            return BadRequest(ModelState);

        return Ok(moneySpent);
    }
}