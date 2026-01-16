using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace CheapAsChips.Controllers
{
    public class NewRecipeController : Controller
    {
        // GET: NewRecipe
        public ActionResult Index()
        {
            return View();
        }

        // GET: NewRecipe/Details/5
        public ActionResult Details(int id)
        {
            return View();
        }

        // GET: NewRecipe/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: NewRecipe/Create
        [HttpPost]
        public ActionResult Create(FormCollection collection)
        {
            try
            {
                // TODO: Add insert logic here

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }

        // GET: NewRecipe/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: NewRecipe/Edit/5
        [HttpPost]
        public ActionResult Edit(int id, FormCollection collection)
        {
            try
            {
                // TODO: Add update logic here

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }

        // GET: NewRecipe/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: NewRecipe/Delete/5
        [HttpPost]
        public ActionResult Delete(int id, FormCollection collection)
        {
            try
            {
                // TODO: Add delete logic here

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }
    }
}