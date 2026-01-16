using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using App.Models;

namespace App.Controllers
{
    public class ProductsController : Controller
    {
        
       [HttpGet]
        public ActionResult Index()
        {
            if (!User.Identity.IsAuthenticated)
                return RedirectToAction("Login", "Account");
            Product product = new Product();
            return View(product);
        }

        [HttpPost]
        public ActionResult Index(Product product)
        {
            if (!User.Identity.IsAuthenticated)
                return RedirectToAction("Login", "Account");
            return View("Edit", product);
        }
        [HttpPost]
        public ActionResult Edit(Product product)
        {
            if (!User.Identity.IsAuthenticated)
                return RedirectToAction("Login", "Account");

            //тут надо сохранить product в базу
            return View("Save",product);
        }

        public ActionResult Save()
        {
            return View();
        }
      
    }
}