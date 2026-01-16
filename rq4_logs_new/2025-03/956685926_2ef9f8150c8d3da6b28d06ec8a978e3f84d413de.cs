using EventSystem.API.Dtos.EvenntsDto;
using Microsoft.AspNetCore.Authorization;
using Microsoft.EntityFrameworkCore;

namespace EventSystem.API.Controllers;

[Route("api/[controller]")]
[ApiController]
public class EventsController : ControllerBase
{
    private readonly ApplicationDbContext _context;

    public EventsController(ApplicationDbContext context)
    {
        _context = context;
    }

    [HttpGet]
    public async Task<ActionResult<IEnumerable<Event>>> GetEvents()
    {
        return Ok(await _context.Events.ToListAsync());
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<Event>> GetEvent(int id)
    {
        var _event = await _context.Events.FindAsync(id);
        if (_event == null)
        {
            return NotFound(new { Message = $"The Event with Id {id}  is not exit" });
        }
        return Ok(_event);
    }

    [HttpPost]
    [Authorize(Roles = "Administrator")]
    public async Task<ActionResult<Event>> CreateEvent(CreateEventDto model)
    {
        if (!ModelState.IsValid)
            return BadRequest(ModelState);

        var newEvent = new Event
        {
            ArtistId = model.ArtistId,
            Date = model.Date,
            Location = model.Location,
            Title = model.Title
        };
        _context.Events.Add(newEvent);
        await _context.SaveChangesAsync();
        return CreatedAtAction(nameof(GetEvent), new { id = newEvent.EventId }, newEvent);
    }

    [HttpPut("{id}")]
    [Authorize(Roles = "Administrator")]
    public async Task<IActionResult> UpdateEvent(int id, Event eventEntity)
    {
        if (id != eventEntity.EventId) return BadRequest();

        _context.Entry(eventEntity).State = EntityState.Modified;
        await _context.SaveChangesAsync();

        return NoContent();
    }

    [HttpDelete("{id}")]
    [Authorize(Roles = "Administrator")]
    public async Task<IActionResult> DeleteEvent(int id)
    {
        var eventEntity = await _context.Events.FindAsync(id);
        if (eventEntity == null) return NotFound();

        _context.Events.Remove(eventEntity);
        await _context.SaveChangesAsync();
        return NoContent();
    }
}