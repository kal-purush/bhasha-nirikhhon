using System;
using System.Collections.Generic;
using Microsoft.AspNet.Mvc;
using Space_Herdsmans.Models;
using Space_Herdsmans.Services;

namespace Space_Herdsmans.Controllers
{
    [Route("api/[controller]")]
    public class UsersLocationController : Controller
    {

        private readonly IUsersLocationService _usersLocationService;

        public UsersLocationController(IUsersLocationService usersLocationService)
        {
            _usersLocationService = usersLocationService;
        }

        [HttpGet("{userId}")]
        public IEnumerable<GeoCoordinates> Get(Guid userId)
        {
            var lastPosition = _usersLocationService.GetLastUserPosition(userId);
            return _usersLocationService.GetCloseUsers(lastPosition, 0);
        }

        [HttpPost("{userId}")]
        public void PostLocation(Guid userId, [FromBody]GeoCoordinates coordinates)
        {
            _usersLocationService.UpdateUserPosition(userId, coordinates);
        }
    }
}