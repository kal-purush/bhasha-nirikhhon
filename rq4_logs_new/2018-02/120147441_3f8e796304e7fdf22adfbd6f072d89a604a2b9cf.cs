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
    public class LeaseController : Controller
    {
        private RBaseContext db = new RBaseContext();

        // GET: Lease
        public ActionResult Index()
        {
            var leases = db.Leases.Include(l => l.Property);
            return View(leases.ToList());
        }

        // GET: Lease/Details/5
        public ActionResult Details(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Lease lease = db.Leases.Find(id);
            if (lease == null)
            {
                return HttpNotFound();
            }
            return View(lease);
        }

        // GET: Lease/Create
        public ActionResult Create()
        {
            ViewBag.PropertyID = new SelectList(db.Properties, "ID", "Street");
            return View();
        }

        // POST: Lease/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see https://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create([Bind(Include = "LeaseID,PropertyID,StartDate,EndDate,RateMonthly")] Lease lease)
        {
            if (ModelState.IsValid)
            {
                db.Leases.Add(lease);
                db.SaveChanges();
                return RedirectToAction("Index");
            }

            ViewBag.PropertyID = new SelectList(db.Properties, "ID", "Street", lease.PropertyID);
            return View(lease);
        }

        // GET: Lease/Edit/5
        public ActionResult Edit(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Lease lease = db.Leases.Find(id);
            if (lease == null)
            {
                return HttpNotFound();
            }
            ViewBag.PropertyID = new SelectList(db.Properties, "ID", "Street", lease.PropertyID);
            return View(lease);
        }

        // POST: Lease/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see https://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit([Bind(Include = "LeaseID,PropertyID,StartDate,EndDate,RateMonthly")] Lease lease)
        {
            if (ModelState.IsValid)
            {
                db.Entry(lease).State = EntityState.Modified;
                db.SaveChanges();
                return RedirectToAction("Index");
            }
            ViewBag.PropertyID = new SelectList(db.Properties, "ID", "Street", lease.PropertyID);
            return View(lease);
        }

        // GET: Lease/Delete/5
        public ActionResult Delete(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Lease lease = db.Leases.Find(id);
            if (lease == null)
            {
                return HttpNotFound();
            }
            return View(lease);
        }

        // POST: Lease/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public ActionResult DeleteConfirmed(int id)
        {
            Lease lease = db.Leases.Find(id);
            db.Leases.Remove(lease);
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