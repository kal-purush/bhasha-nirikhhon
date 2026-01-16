using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using StarterKit.Models;
using StarterKit.Services;

[ApiController]
[Route("api/showdates")]
public class ShowDatesController : ControllerBase
{
    private readonly DatabaseContext _context;

    public ShowDatesController(DatabaseContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<IActionResult> GetShowDates([FromQuery] int showId)
    {
        if (showId <= 0)
        {
            return BadRequest("Invalid show ID.");
        }

        var showDates = await _context.TheatreShowDate
            .Where(date => date.TheatreShowId == showId)
            .Select(date => new
            {
                date.TheatreShowDateId,
                date.DateAndTime
            })
            .ToListAsync();

        if (!showDates.Any())
        {
            return NotFound("No dates available for the selected show.");
        }

        return Ok(showDates);
    }
}