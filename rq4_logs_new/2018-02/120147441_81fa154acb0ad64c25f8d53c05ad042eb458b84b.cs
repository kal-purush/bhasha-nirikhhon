using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using Rentalbase.DAL;
using Rentalbase.Models;

namespace Rentalbase.Controllers
{
    [Authorize(Roles = "Admin")]
    public class LandlordController : Controller
    {
        private RBaseContext db = new RBaseContext();

        // GET: Landlord
        public ActionResult Index()
        {
            // Linq for finding the number of landlords who dont have properties
            ViewBag.NumOfFreeLords =
                (from lord in db.Landlords
                where !(from prop in db.Properties
                        select prop.LandlordID).Contains(lord.ID)
                select db.Landlords.Count()).SingleOrDefault();

            return View(db.Landlords.ToList());
        }

        // GET: Landlord/Details/5
        public ActionResult Details(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Landlord landlord = db.Landlords.Find(id);
            if (landlord == null)
            {
                return HttpNotFound();
            }
            return View(landlord);
        }

        // GET: Landlord/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Landlord/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see https://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create([Bind(Include = "ID,Name,Street,City,State,Zip,Phone,Email")] Landlord landlord)
        {
            if (ModelState.IsValid)
            {
                db.Landlords.Add(landlord);
                db.SaveChanges();
                return RedirectToAction("Index");
            }

            return View(landlord);
        }

        // GET: Landlord/Edit/5
        public ActionResult Edit(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Landlord landlord = db.Landlords.Find(id);
            if (landlord == null)
            {
                return HttpNotFound();
            }
            return View(landlord);
        }

        // POST: Landlord/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see https://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit([Bind(Include = "ID,Name,Street,City,State,Zip,Phone,Email")] Landlord landlord)
        {
            if (ModelState.IsValid)
            {
                db.Entry(landlord).State = EntityState.Modified;
                db.SaveChanges();
                return RedirectToAction("Index");
            }
            return View(landlord);
        }

        // GET: Landlord/Delete/5
        public ActionResult Delete(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Landlord landlord = db.Landlords.Find(id);
            if (landlord == null)
            {
                return HttpNotFound();
            }
            return View(landlord);
        }

        // POST: Landlord/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public ActionResult DeleteConfirmed(int id)
        {
            Landlord landlord = db.Landlords.Find(id);
            db.Landlords.Remove(landlord);
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