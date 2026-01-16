using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MonsterHotel.Models;
using MonsterHotel.Repositories;

namespace MonsterHotel.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserProfileController : ControllerBase
    {
        private readonly IUserProfileRepository _userProfileRepository;
        public UserProfileController(IUserProfileRepository userProfileRepository)
        {
            _userProfileRepository = userProfileRepository;
        }

        [HttpGet]
        public IActionResult Get()
        {
            return Ok(_userProfileRepository.GetAll());
        }

        [HttpGet("{firebaseUserId}")]
        public IActionResult GetByFirebaseUserId(string firebaseUserId)
        {
            var userProfile = _userProfileRepository.GetByFireBaseId(firebaseUserId);
            if (userProfile == null)
            {
                return NotFound();
            }
            return Ok(userProfile);
        }

        [HttpGet("DoesUserExist/{firebaseUserId}")]
        public IActionResult DoesUserExist(string firebaseUserId)
        {
            var userProfile = _userProfileRepository.GetByFireBaseId(firebaseUserId);
            if (userProfile == null)
            {
                return NotFound();
            }
            return Ok();
        }

        //private UserProfile GetCurrentUserProfile()
        //{
        //    var firebaseUserId = User.FindFirst(ClaimTypes.NameIdentifier).Value;
        //    return _userProfileRepository.GetByFireBaseId(firebaseUserId);
        //}

        //[HttpPost]
        //public IActionResult Register(UserProfile userProfile)
        //{
        //    // All newly registered users start out as a "user" user type (i.e. they are not admins)
        //    userProfile.UserTypeId = UserType.USER_TYPE_ID;
        //    _userProfileRepository.Add(userProfile);
        //    return CreatedAtAction(
        //        nameof(GetByFireBaseId), new { firebaseUserId = userProfile.FireBaseId }, userProfile);
        //}
    }
}