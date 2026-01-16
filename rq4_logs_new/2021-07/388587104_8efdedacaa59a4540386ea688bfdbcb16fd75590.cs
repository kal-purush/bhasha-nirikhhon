using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Security.Claims;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MonsterHotel.Models;
using MonsterHotel.Repositories;

namespace MonsterHotel.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StayController : ControllerBase
    {
        private readonly IStayRepository _stayRepository;
        private readonly IUserProfileRepository _userProfileRepository;
        public StayController(IStayRepository stayRepository)
        {
            _stayRepository = stayRepository;
        }

        [HttpGet]
        public IActionResult Get()
        {
            return Ok(_stayRepository.GetAll());
        }
        [HttpGet("{id}")]
        public IActionResult Get(int id)
        {
            var stay = _stayRepository.GetById(id);
            if (stay == null)
            {
                return NotFound();
            }
            return Ok(stay);
        }
        [HttpGet("Deactivated")]
        public IActionResult GetAllDeactivated()
        {
            return Ok(_stayRepository.GetAllDeactivated());
        }
        [HttpDelete("Delete/{id}")]
        public IActionResult Deactivate(int id)
        {
            _stayRepository.Deactivate(id);
            return NoContent();
        }
        [HttpPut("Activate/{id}")]
        public IActionResult Activate(int id)
        {
            _stayRepository.Activate(id);
            return NoContent();
        }
        [HttpPost]
        public IActionResult Add(Stay stay)
        {
            var currentUser = GetCurrentUserProfile();
            if (currentUser.UserType.Name != "admin")
            {
                return Unauthorized();
            }
            //stay.CheckInTime = DateTime.Now;
            //stay.PublishDateTime = DateTime.Now;
            stay.HandlerId = currentUser.Id;
            stay.IsActive = false;
            _stayRepository.Add(stay);
            return CreatedAtAction(nameof(Get), new { id = stay.Id }, stay);
        }
        [HttpPut("CheckIn/{id}")]
        public IActionResult CheckIn(Stay stay)
        {
            var currentUser = GetCurrentUserProfile();
            if (currentUser.UserType.Name != "guest")
            {
                return Unauthorized();
            }
            stay.CheckInTime = DateTime.Now;
            //stay.PublishDateTime = DateTime.Now;
            stay.GuestId = currentUser.Id;
            stay.IsActive = true;
            _stayRepository.CheckIn(stay);
            return CreatedAtAction(nameof(Get), new { id = stay.Id }, stay);
        }

        private UserProfile GetCurrentUserProfile()
        {
            var firebaseUserId = User.FindFirst(ClaimTypes.NameIdentifier).Value;
            return _userProfileRepository.GetByFireBaseId(firebaseUserId);
        }
    }
}