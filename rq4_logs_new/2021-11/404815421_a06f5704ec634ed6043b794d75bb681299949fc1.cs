using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebWasted.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class LoginController : ControllerBase
    {
        [HttpPost]
        public int Post(String username, String password)
        {
            using(DataContext context = new DataContext())
            {
                var findUser = (from user in context.Users where user.UserName == username && user.Password == password select user).First();
                if(findUser == null)
                {
                    return -1;
                }
                return findUser.ID;
            }
        }
    }
}