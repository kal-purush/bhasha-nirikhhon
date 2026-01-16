using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using Brewery_Tracker.Models;

namespace Brewery_Tracker.Controllers
{
    public class BreweriesCRUDController : Controller
    {
        private BreweryFactory db = new BreweryFactory();

        // GET: BreweriesCRUD
        public ActionResult Index()
        {
            return View(db.Breweries.ToList());
        }

        // GET: BreweriesCRUD/Details/5
        public ActionResult Details(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Brewery brewery = db.Breweries.Find(id);
            if (brewery == null)
            {
                return HttpNotFound();
            }
            return View(brewery);
        }

        // GET: BreweriesCRUD/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: BreweriesCRUD/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create([Bind(Include = "Brewery_ID,Brewery_Name,Brewery_Description,Brewery_Address,Brewery_City,Brewery_State,Brewery_Zip,Brewery_Rating")] Brewery brewery)
        {
            if (ModelState.IsValid)
            {
                db.Breweries.Add(brewery);
                db.SaveChanges();
                return RedirectToAction("Index");
            }

            return View(brewery);
        }

        // GET: BreweriesCRUD/Edit/5
        public ActionResult Edit(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Brewery brewery = db.Breweries.Find(id);
            if (brewery == null)
            {
                return HttpNotFound();
            }
            return View(brewery);
        }

        // POST: BreweriesCRUD/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit([Bind(Include = "Brewery_ID,Brewery_Name,Brewery_Description,Brewery_Address,Brewery_City,Brewery_State,Brewery_Zip,Brewery_Rating")] Brewery brewery)
        {
            if (ModelState.IsValid)
            {
                db.Entry(brewery).State = EntityState.Modified;
                db.SaveChanges();
                return RedirectToAction("Index");
            }
            return View(brewery);
        }

        // GET: BreweriesCRUD/Delete/5
        public ActionResult Delete(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Brewery brewery = db.Breweries.Find(id);
            if (brewery == null)
            {
                return HttpNotFound();
            }
            return View(brewery);
        }

        // POST: BreweriesCRUD/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public ActionResult DeleteConfirmed(int id)
        {
            Brewery brewery = db.Breweries.Find(id);
            db.Breweries.Remove(brewery);
            db.SaveChanges();
            return RedirectToAction("Index");
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                db.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}