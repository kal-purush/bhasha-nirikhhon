using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using server.Data;
using server.Entities;

namespace server.Controllers;

[ApiController]
[Route("server/[controller]")]
public class EventsController : BaseApiController
{
    private readonly StoreContext _context;
    public EventsController(StoreContext context)
    {
        this._context = context;
    }

    [HttpGet]
    public async Task<ActionResult<List<Event>>> GetEvents()
    {
        return await _context.Events.ToListAsync();
    }

    [HttpGet("{id}")]
    public async Task<ActionResult<Event>> GetEvent(int id)
    {
        return await _context.Events.FindAsync(id);
    }
}