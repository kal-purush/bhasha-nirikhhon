using Microsoft.AspNetCore.Mvc;

namespace FoodImportStickerGenerator.Controllers;

[ApiController]
[Route("[controller]")]
public class StickerController : ControllerBase
{
    private static readonly string[] Summaries = new[]
    {
        "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
    };

    private readonly ILogger<StickerController> _logger;

    public StickerController(ILogger<StickerController> logger)
    {
        _logger = logger;
    }

    [HttpGet(Name = "GetWeatherForecast")]
    public IEnumerable<Sticker> Get()
    {
        return Enumerable.Range(1, 5).Select(index => new Sticker
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
    }
}