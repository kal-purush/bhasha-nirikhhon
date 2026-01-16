using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using TokTok.Models;
using TokTok.Repositories;

namespace TokTok.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class UsersController
    {
        private readonly IUserRepository _userRepository;

        public UsersController(IUserRepository userRepository)
        {
            _userRepository = userRepository;
        }

        // GET api/values
        [HttpGet]
        public ActionResult<IEnumerable<User>> Get()
        {
            return _userRepository.GetUsers();
        }

        // POST api/values
        [HttpPost]
        public void Post([FromBody] User newUser)
        {
            // Basic version of incrementation
            var userAmount = _userRepository.GetUsers().Count;
            var id = _userRepository.GetUsers()[userAmount - 1].Id;
            newUser.Id = ++id;
            _userRepository.Create(newUser);
        }
    }
}