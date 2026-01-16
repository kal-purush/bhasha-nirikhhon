using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using neutrackAPI.Data;
using neutrackAPI.Models;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace neutrackAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UsersController : ControllerBase
    {
        private readonly IUserRepository _userRepository;

        public UsersController(IUserRepository userRepository)
        {
            _userRepository = userRepository;
        }
        /// <summary>
        /// GET api/users
        /// </summary>
        /// <returns>A list of Users</returns>
        [HttpGet]
        public ActionResult<IEnumerable<User>> GetUsers()
        {
            var userItems = _userRepository.GetAllUsers();
            return Ok(userItems);
        }

        /// <summary>
        /// GET api/users/2
        /// </summary>
        /// <param name="id"></param>
        /// <returns>A user matching the id parameter</returns>
        [HttpGet("{id}")]
        public ActionResult<User> GetUserById(int id)
        {
            var userItem = _userRepository.GetUserById(id);
            return Ok(userItem);
        }

        // POST api/values
        [HttpPost]
        public void Post([FromBody] string value)
        {
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}