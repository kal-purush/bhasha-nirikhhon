using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using wad.Models;

namespace wad.Controllers
{
    public class AccountController : Controller
    {
        // GET: Account
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult Register()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Register(UserAccount account)
        {
            if(ModelState.IsValid)
            {
                using (UserContext db = new UserContext())
                {
                    db.userAccount.Add(account);
                    db.SaveChanges();
                }

                ModelState.Clear();
                ViewBag.Message = account.userName + " registered";
            }

            return View();
        }

        public ActionResult Login()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Login(UserAccount user)
        {
            using(UserContext db = new UserContext())
            {
                var usr = db.userAccount.Where(u => u.userName == user.userName && u.userPw == user.userPw).FirstOrDefault();

                if(usr != null)
                {
                    Session["userId"] = usr.userId.ToString();
                    Session["userName"] = usr.userName.ToString();

                    return RedirectToAction("LoggedIn");
                }
                else
                {
                    ModelState.AddModelError("", "Username or Password incorrect");
                }
            }

            return View();
        }

        public ActionResult LoggedIn()
        {
            if(Session["userId"] != null)
            {
                return View();
            }
            else
            {
                return RedirectToAction("Login");
            }
        }
    }
}