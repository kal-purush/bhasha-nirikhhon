using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Studio_Inventory_API;
using Studio_Inventory_API.Models;

namespace Studio_Inventory_API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {
        IRepository<User>_userRepo;

        public UserController(IRepository<User> userRepo)
        {
            this._userRepo = userRepo;
        }

        // GET: api/User
        [HttpGet]
        public IEnumerable<User> GetUsers()
        {
            return _userRepo.GetAll();
        }

        // GET: api/User/5
        [HttpGet("{id}")]
        public User GetUser(int id)
        {
            var user = _userRepo.GetById(id);
            return user;
        }

        // PUT: api/User/5
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.
        [HttpPut("{id}")]
        public User PutUser(int id, User user)
        {
            if (id != user.Id)
            {
                return null;
            }

            _userRepo.Update(user);
            return user;
        }

        // POST: api/User
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.
        [HttpPost]
        public User PostUser([FromBody] User user)
        {
            _userRepo.Create(user);
            return user;
        }

        // DELETE: api/User/5
        [HttpDelete("{id}")]
        public string DeleteUser(int id)
        {
            var user = _userRepo.GetById(id);
            _userRepo.Delete(user);
            return "User deleted.";
        }

        //private bool UserExists(int id)
        //{
        //    return _context.Users.Any(e => e.Id == id);
        //}
    }
}