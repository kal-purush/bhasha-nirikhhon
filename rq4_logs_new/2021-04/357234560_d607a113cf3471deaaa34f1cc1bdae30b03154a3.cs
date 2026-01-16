using Backend.Models;
using Backend.Services;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;

namespace Backend.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class RoomsController : ControllerBase
    {
        private readonly RoomService _roomService;

        public RoomsController(RoomService roomService)
        {
            _roomService = roomService;
        }

        [HttpGet]
        public ActionResult<List<Room>> Get() =>
            _roomService.Get();

        [HttpGet("{id:length(24)}", Name = "GetRoom")]
        public ActionResult<Room> Get(string id)
        {
            var room = _roomService.Get(id);

            if (room == null)
            {
                return NotFound();
            }

            return room;
        }

        [HttpPost]
        public ActionResult<Room> Create(Room room)
        {
            _roomService.Create(room);

            return CreatedAtRoute("GetRoom", new { id = room.Id.ToString() }, room);
        }

        [HttpPut("{id:length(24)}")]
        public IActionResult Update(string id, Room roomIn)
        {
            var room = _roomService.Get(id);

            if (room == null)
            {
                return NotFound();
            }

            _roomService.Update(id, roomIn);

            return NoContent();
        }

        [HttpDelete("{id:length(24)}")]
        public IActionResult Delete(string id)
        {
            var room = _roomService.Get(id);

            if (room == null)
            {
                return NotFound();
            }

            _roomService.Remove(room.Id);

            return NoContent();
        }
    }

}