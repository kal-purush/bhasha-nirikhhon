using Microsoft.AspNetCore.Mvc;
using api_finport.Models;
using System.Linq;
using System.Collections.Generic;

namespace api_finport.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UsersController : ControllerBase
    {
        private readonly FinPortContext _context;

        public UsersController(FinPortContext context)
        {
            _context = context;
            if (_context.Users.Count() == 0)
            {
                _context.Users.Add(new Models.User()
                {
                    Username = "bleonardo",
                    FirstName = "Bruno",
                    LastName = "Leonardo",
                    Password = "123",
                    Email = "brunoleonardo1@gmail.com"
                });
                _context.SaveChanges();
            }
        }

        [HttpGet]
        public ActionResult<List<User>> GetAll()
        {
            return _context.Users.ToList();
        }

        [HttpGet("{id}", Name="GetTodo")]
        public ActionResult<User> GetById(int id){
            User user = _context.Users.Find(id);
            if(user == null)
                return NotFound();
            return user;
        }

    }
}