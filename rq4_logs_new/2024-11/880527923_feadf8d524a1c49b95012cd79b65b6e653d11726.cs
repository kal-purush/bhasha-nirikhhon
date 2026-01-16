using Humanizer.DateTimeHumanizeStrategy;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MonezyAPI.Data;
using MonezyAPI.Models;

namespace MonezyAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UsersController : Controller
    {
        private readonly UsersContext usersContext;

        public UsersController(UsersContext usersContext) 
        {
            this.usersContext = usersContext;
        }

        [HttpGet]
        public async Task<ActionResult<Users>> GetUsers()
        {
            return new Users { };
        }

        [HttpPost]
        public async Task<ActionResult<Users>> InsertUsers(Users user)
        {
            usersContext.Users.Add(user);
            await usersContext.SaveChangesAsync();

            return CreatedAtAction(nameof(InsertUsers), new { id = user.IdUser }, user);
        }

    }
}