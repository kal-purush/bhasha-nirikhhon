using AutoMapper;
using Event_Planning_System.DTO;
using Event_Planning_System.IServices;
using Microsoft.AspNetCore.Mvc;

namespace Event_Planning_System.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	public class ToDoList : ControllerBase
	{
		private readonly IToDoListService toDoListService;
		public ToDoList(IToDoListService _toDoListService, IMapper _mapper)
		{
			toDoListService = _toDoListService;
		}
		[HttpPost]
		public async Task<IActionResult> CreateToDoList(ToDoListDTO newToDoListDTO)
		{
			if (ModelState.IsValid)
			{
				if (await toDoListService.CreateToDoList(newToDoListDTO))
					return Created();
				else
					return BadRequest("Failed to create to do list. Invalid data or deadline is in the past.");
			}
			return BadRequest(ModelState);
		}
		[HttpGet]
		public async Task<IActionResult> Get()
		{
			var toDoLists = await toDoListService.GetAllToDoLists();
			if (toDoLists == null)
				return NotFound("No to do lists found.");
			return Ok(toDoLists);
		}
		[HttpDelete]
		public async Task<IActionResult> Delete(int eventId,string name)
		{
			if (await toDoListService.DeleteToDoListSoft(eventId,name))
				return Created();
			return BadRequest();
		}
		[HttpPut]
		public async Task<IActionResult> Update(int eventId, string name, ToDoListDTO newToDoList)
		{
			if (ModelState.IsValid)
			{
				if (await toDoListService.UpdateToDoList(eventId, name, newToDoList))
					return Created();
				else
					return BadRequest("Failed to update to do list. Invalid data or to do list does not exist.");
			}
			return BadRequest(ModelState);
		}



	}
}