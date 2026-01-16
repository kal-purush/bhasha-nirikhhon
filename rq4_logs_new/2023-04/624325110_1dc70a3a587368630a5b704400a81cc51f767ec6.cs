using LoginApi.Datas;
using LoginApi.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace LoginApi.Controllers
{
    [ApiController]
    [Route("api/[controller]/[action]")]
    public class LogincredController : ControllerBase
    {
        private readonly UserDbContext _userDbContext;
        public LogincredController(UserDbContext dbContext)
        {
            _userDbContext = dbContext;
        }
        [HttpPost]
        public ActionResult<UserLoginClass> Create([FromBody] UserLoginClass newuser)
        {
            _userDbContext.Logincred.Add(newuser);
            _userDbContext.SaveChanges();
            return Ok();
        }
        [HttpPost("authentication")]
        public IActionResult Authentication([FromBody]UserLoginClass checkuser)
        {
            var auth = _userDbContext.Logincred.FirstOrDefault(x => x.Username == checkuser.Username && x.Password == checkuser.Password);
            if (auth == null)
            {
                return BadRequest();
            }
            return Ok(auth);
        }
        [HttpGet]
        public List<UserLoginClass> Get( )
        {
            return _userDbContext.Logincred.ToList();
        }
        [HttpDelete]
        public IActionResult Delete(int id)
        {
            var deluser= _userDbContext.Logincred.FirstOrDefault(x => x.Id == id);
            if (deluser == null)
            {
                return BadRequest();
            }
            _userDbContext.Remove(deluser);
            _userDbContext.SaveChanges();
            return Ok();
        }
    }
}