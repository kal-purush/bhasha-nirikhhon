using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MVCswitchback.Data;

namespace MVCswitchback.Controllers
{
    public class UserInfoController : Controller
    {
        private readonly SwitchbackDbContext _context;

        public UserInfoController(SwitchbackDbContext context)
        {
            _context = context;
        }



        [HttpGet]
        public IActionResult Index()
        {
            return View();
        }
    }
}