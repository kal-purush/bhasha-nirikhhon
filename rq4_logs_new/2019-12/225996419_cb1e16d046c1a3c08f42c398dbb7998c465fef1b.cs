using System.Linq;
using HumansInHarmony.Models;
using Microsoft.AspNetCore.Mvc;

namespace HumansInHarmony.Controllers
{
    public class LoginController : Controller
    {
        public readonly SongContext _context = new SongContext();
        public SongContext db = new SongContext();
        [HttpPost]
        public IActionResult Register(User u)
        {
            TempData["UserName"] = u.Email;
            db.Add(u);
            db.SaveChanges();
            return View(u);
        }

        public IActionResult Register()
        {
            return View();
        }

        public IActionResult UserLogin()
        {
            return View();
        }
        [HttpPost]
        public IActionResult UserLogin(User FormUser)
        {
            User dbu = _context.User.ToList().Find(u => u.Email == FormUser.Email);

            if (dbu == null)
            {
                ViewBag.Error = "Invalide Email or Password";
                return View();
            }
            else if (FormUser.Password == dbu.Password)
            {
                TempData["UserName"] = FormUser.Email;
                ViewBag.Name = FormUser.Email;
                return RedirectToAction("HomePage");
            }
            else
            {
                return RedirectToAction("UserLogin");
            }
        }

        public IActionResult Logout()
        {
            return View();
        }

    }
}