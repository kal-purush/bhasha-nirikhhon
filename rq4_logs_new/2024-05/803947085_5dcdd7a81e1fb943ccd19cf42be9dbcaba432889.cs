using Event_Planning_System.IServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Event_Planinng_System_DAL.Models;

namespace Event_Planning_System.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class EventController : ControllerBase
	{
		private readonly IEventService eventService;
		public EventController(IEventService _eventService)
		{
			eventService = _eventService;
		}
		[HttpPost]
		public IActionResult CreateEvent(Event newEvent)
		{
			if (eventService.CreateEvent(newEvent))
				return Created();
			return BadRequest();
		}
	}
}