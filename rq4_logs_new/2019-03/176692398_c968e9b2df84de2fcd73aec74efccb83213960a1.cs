using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CoreEcommerceUserPanal.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CoreEcommerceUserPanal.Controllers
{
    public class CustomersController : Controller
    {
        ShoppingProjectContext context = new ShoppingProjectContext();
        public IActionResult Index()
        {
            return View();
        }
        [HttpGet]
        public ViewResult Register()
        {
            return View();
        }
        [HttpPost]
        public ActionResult Register(Customers cust)
        {
            context.Customers.Add(cust);
            context.SaveChanges();

            return RedirectToAction("Login");
        }
        public IActionResult Login()
        {
            return View();
        }
        
        [HttpPost]

        public ActionResult Login(string username, string password)
        {


            var user = context.Customers.Where(a => a.UserName == username).SingleOrDefault();
            ViewBag.cust = user;
            if (user == null)
            {
                ViewBag.Error = "Invalid Credentials";
                return View("Index");
            }
            else
            {
                var userName = user.UserName;
                int custId = ViewBag.cust.CustomerId;
                if(username !=null && password !=null && username.Equals(userName) && password.Equals("12345"))
                {
                    HttpContext.Session.SetString("uname", username);
                    return RedirectToAction("checkout", "cart", new
                    {
                        @id = custId
                    });
                }
                else
                {
                    ViewBag.Error = "Invalid credentials";
                    return View("Index");
                }
            }
        }
        [Route("Logout")]
        [HttpGet]
        public IActionResult Logout()
        {
            HttpContext.Session.Remove("uname");
            return RedirectToAction("Index");
        }
    }
}