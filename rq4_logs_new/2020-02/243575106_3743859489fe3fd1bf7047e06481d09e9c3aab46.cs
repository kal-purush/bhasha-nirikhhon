using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using AutoMapper;
using file.Api.Resources;
using file.Core.Models;
using file.Core.Services;
using Microsoft.AspNetCore.Mvc;

namespace file.Api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {
        private readonly IUserService _userService;
        private readonly IMapper _mapper;

        public UserController(IUserService userService, IMapper mapper)
        {
            _userService = userService;
            _mapper = mapper;
        }

        public async Task<ActionResult<IEnumerable<UserResource>>> GetUsers()
        {
            var users = await _userService.GetAllUsers();
            var usersResult = _mapper.Map<IEnumerable<User>,IEnumerable<UserResource>>(users);
            return Ok(usersResult);
        }
    }
}