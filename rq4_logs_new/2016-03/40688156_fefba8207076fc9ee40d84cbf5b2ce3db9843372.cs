using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using MesjidCommittee.Models;
using MesjidCommittee.ViewModels;
using MesjidCommittee.DAL;
using MesjidCommittee.Helpers;
using MesjidCommittee.Helpers.ServerResponse;
using MesjidCommittee.Repositories;

namespace MesjidCommittee.Controllers
{
    public class UserAccountController : Controller
    {
        private UserAccountRepo userAccountRepo = new UserAccountRepo();
        public ActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Login(UserAccount user)
        {
            return Json(userAccountRepo.validateLogin(user));
        }

        [HttpPost]
        public ActionResult Register(UserAccount user)
        {
            return Json(userAccountRepo.validateRegister(user));
        }
    }
}


