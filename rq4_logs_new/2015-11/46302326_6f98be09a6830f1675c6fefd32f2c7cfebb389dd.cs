using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Web;
using System.Web.Mvc;
using DAO;
using Microsoft.AspNet.Identity;
using Microsoft.AspNet.Identity.EntityFramework;
using Microsoft.AspNet.Identity.Owin;
using Services.Interfaces;
using WebApp.Models;
using WebApp.Models.Domains;

namespace WebApp.Controllers
{
    [Authorize]
    public class UserController : Controller
    {
        //private readonly ApplicationUser _logginedUser;
        private readonly IPersonService _personService;

        public UserController(IPersonService personService)
        {
            _personService = personService;
        }

        [HttpGet]
        public ActionResult Index()
        {
            return View();
        }

        [HttpGet]
        public ActionResult UserInfo()
        {
            var userId = Guid.Parse(User.Identity.GetUserId());
            var user = _personService.Get(userId);

            var model = new UserInfoViewModel
            {
                Email = user?.Login ?? User.Identity.Name,
                Name = user?.Name ?? "",
                LastName = user?.LastName ?? "",
                SecondName = user?.SecondName ?? "",
                DateBirth = user?.DateBirth ?? DateTime.Now,
                Number = user?.PassportSet.Number ?? "",
                RUVD = user?.PassportSet.RUVD ?? "",
                Adress = user?.PassportSet.Adress ?? "",
                Validity = user?.PassportSet.Validity ?? DateTime.Now
            };

            return View(model);
        }

        [HttpPost]
        public ActionResult UserInfo(UserInfoViewModel model)
        {
            if (!ModelState.IsValid)
            {
                return View(model);
            }

            var userId = Guid.Parse(User.Identity.GetUserId());
            var person = new PersonSet()
            {
                PersonId = userId,
                Login = model.Email,
                Name = model.Name,
                LastName = model.LastName,
                SecondName = model.SecondName,
                DateBirth = model.DateBirth
            };

            var pasport = new PassportSet()
            {
                Number = model.Number,
                RUVD = model.RUVD,
                Adress = model.Adress,
                Validity = model.Validity
            };

            _personService.RegisterUser(person, pasport);

            return View(model);
        }
    }

    //var store = new UserStore<ApplicationUser>(new ApplicationDbContext());
    //var userManager = new UserManager<ApplicationUser>(store);
    //var test = User.Identity;
    //ApplicationUser logginedUser = userManager.FindById(User.Identity.GetUserId());

    //ApplicationUser user = System.Web.HttpContext.Current.GetOwinContext().GetUserManager<ApplicationUserManager>().FindById(System.Web.HttpContext.Current.User.Identity.GetUserId());
}