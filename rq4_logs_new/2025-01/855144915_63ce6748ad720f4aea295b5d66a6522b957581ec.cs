using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

[Route("api/v1/Weather")]
[ApiController]
public class WeatherController : ControllerBase
{
    private readonly WeatherService _weatherService;

    // Inject WeatherService via dependency injection
    public WeatherController(WeatherService weatherService)
    {
        _weatherService = weatherService;
    }

    // Endpoint to get weather for a specific city (latitude and longitude)
    [HttpGet()]
    public async Task<ActionResult<WeatherResponse>> GetWeather()
    {
        try
        {
            var weatherData = await _weatherService.GetWeatherAsync();
            // var weatherData = await _weatherService.GetKey();
            return Ok(weatherData);  // Return weather data as a JSON response
        }
        catch (Exception ex)
        {
            // Handle any errors that might occur (e.g., invalid city, API issues)
            return StatusCode(500, $"Internal server error: {ex.Message}");
        }
    }
}