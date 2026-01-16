using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Identity;
using Newtonsoft.Json.Linq;

namespace AspNetCore.Identity.DocumentDB.Sample.Controllers
{
    [Route("api/[controller]")]
    public class ValuesController : Controller
    {
        private UserManager<MyUser> _userManager;

        public ValuesController(UserManager<MyUser> userManager)
        {
            _userManager = userManager;
        }

        // GET api/values
        [HttpGet]
        public IEnumerable<MyUser> Get()
        {
            return _userManager.Users.AsEnumerable();
        }

        // GET api/values/5
        [HttpGet("{value}")]
        public async Task<MyUser> Get(string value)
        {
            MyUser user = await _userManager.FindByNameAsync(value);
            return user;
        }

        // POST api/values
        [HttpPost("{userName}")]
        public void Post(string userName)
        {
            MyUser user = new MyUser
            {
                UserName = userName,
                Email = userName + "@test.domain",
                CustomProperty = "custom value"
            };

            _userManager.CreateAsync(user, "89hT%)password43270"); // just for testing purposes
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}