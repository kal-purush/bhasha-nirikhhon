using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using NeedyBuddy.Data;
using NeedyBuddy.Models;
using NeedyBuddy.Areas.Identity.Pages.Account;
using Microsoft.AspNetCore.Routing;

namespace NeedyBuddy.Controllers
{
    public class UserRegistrationController : Controller
    {
        private readonly ApplicationDbContext _adc;
        public UserRegistrationController(ApplicationDbContext adc)
        {
            _adc = adc;
        }

        /*public IActionResult Index()
        {
            return View();
        }*/
        [HttpPost]
        public IActionResult Create(Models.RegisterModel ud) 
        {
            _adc.Add(ud);
            _adc.SaveChanges();
            return new RedirectToRouteResult(
                new RouteValueDictionary(
                    new
                    {
                        //area = "Administration", /Identity/Account/Register
                        controller = "Home",
                        action = "Index"
                    }
                )
            );
        }
        
    }
}