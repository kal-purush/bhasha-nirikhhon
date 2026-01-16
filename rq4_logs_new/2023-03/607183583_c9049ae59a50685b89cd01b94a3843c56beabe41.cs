using dbcontext;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Linktindr_be.Controllers {
    [Route("api/[controller]")]
    [ApiController]
    public class UsersController : ControllerBase {

        OurContext OU;
        public UsersController(OurContext oU) {
            OU = oU;
        }

        // GET: api/<UsersController>
        [HttpGet]
        public IEnumerable<Users> Get() {
            return OU.users;
        }

        // GET api/<UsersController>/5
        [HttpGet("{id}")]
        public Users Get(int id) {
            Users u = OU.users.Find(id);

            return u;
        }

        // POST api/<UsersController>
        [HttpPost("add")]
        public string Add(Users_noid uni) {
            Users u = new Users();
            u.email = uni.email;
            u.password = uni.password;
            u.usertype = uni.usertype;
            //OU.Add(u);
            //OU.SaveChanges();
            return "gelukt. Email is: " + u.email + " en password is: " + u.password;
        }

        [HttpPost("check")]
        public string Check([FromBody] Users_noid uni) {
            Users u = new Users();
            u.email = uni.email;
            u.password = uni.password;
            u.usertype = uni.usertype;
            //OU.Add(u);
            //OU.SaveChanges();
            return "gelukt. Email is: "+u.email+" en password is: "+u.password;
        }

        // PUT api/<UsersController>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value) {
        }

        // DELETE api/<UsersController>/5
        [HttpDelete("{id}")]
        public void Delete(int id) {
        }
    }
}