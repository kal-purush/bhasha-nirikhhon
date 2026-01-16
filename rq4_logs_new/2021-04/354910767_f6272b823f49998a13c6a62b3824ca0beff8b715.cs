using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Studio_Inventory_API;
using Studio_Inventory_API.Extensions;
using Studio_Inventory_API.Models;
using Studio_Inventory_API.Repositories;

namespace Studio_Inventory_API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AccountController : ControllerBase
    {
        IRepository<User> _accountRepo;

        public AccountController(IRepository<User> accountRepo)
        {
            this._accountRepo = accountRepo;
        }
        // GET: api/Account


        // GET: api/Account/5


        // PUT: api/Account/5
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.


        // POST: api/Account
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.
        [HttpPost]
        public LoginResult CheckLogin([FromBody] User myUser)
        {
            var result = _accountRepo.CheckLogin(myUser.Name, myUser.Password);
            return result;
        }


        // DELETE: api/Account/5

        

      
    }
}