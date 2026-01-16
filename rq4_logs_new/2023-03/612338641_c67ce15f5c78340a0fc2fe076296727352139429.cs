using API.Dtos;
using API.Extensions;
using AutoMapper;
using Core.Entities;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace API.Controllers
{
    public class UsersController : BaseApiController
    {
        private readonly UserManager<AppUser> _userManager;
        private readonly IMapper _mapper;
        public UsersController(UserManager<AppUser> userManager, IMapper mapper)
        {
            _mapper = mapper;
            _userManager = userManager;
        }

        [HttpGet]
        public async Task<ActionResult<UserToReturnDto>> GetCurrentUser()
        {
            var user = await _userManager.Users
                .SingleOrDefaultAsync(x => x.Id == User.GetUserId());
            
            if (user == null) return NotFound("This user was not found");
            
            return Ok(_mapper.Map<AppUser, UserToReturnDto>(user));
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<UserToReturnDto>> GetUserById(int id)
        {
            var user = await _userManager.Users
                .SingleOrDefaultAsync(x => x.Id == id);
            
            if (user == null) return NotFound("This user was not found");
            
            return Ok(_mapper.Map<AppUser, UserToReturnDto>(user));
        }
    }

}