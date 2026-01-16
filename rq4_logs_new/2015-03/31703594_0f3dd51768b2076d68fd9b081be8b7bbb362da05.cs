using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace projectWebKassa.Controllers
{
    public class ProductViewManagerController : Controller
    {
        // GET: productViewManager
        public ActionResult Index()
        {
            return View();
        }

        //

        // GET: productViewManager/Details/5
        public ActionResult Details(int id)
        {
            return View();
        }

        // GET: productViewManager/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: productViewManager/Create
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

        // GET: productViewManager/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: productViewManager/Edit/5
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

        // GET: productViewManager/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: productViewManager/Delete/5
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