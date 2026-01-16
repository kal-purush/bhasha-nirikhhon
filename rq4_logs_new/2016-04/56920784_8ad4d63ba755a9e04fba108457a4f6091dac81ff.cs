using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNet.Mvc;
using SpaceHerders.Web.Models;
using SpaceHerders.Web.Services;

namespace SpaceHerders.Web.Controllers
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
        public async Task<IEnumerable<GeoCoordinates>> Get(Guid userId)
        {
            var lastPosition = await _usersLocationService.GetLastUserPosition(userId);
            return await _usersLocationService.GetCloseUsers(lastPosition, 0);
        }

        [HttpPost("{userId}")]
        public async void PostLocation(Guid userId, [FromBody]GeoCoordinates coordinates)
        {
            await _usersLocationService.UpdateUserPosition(userId, coordinates);
        }
    }
}