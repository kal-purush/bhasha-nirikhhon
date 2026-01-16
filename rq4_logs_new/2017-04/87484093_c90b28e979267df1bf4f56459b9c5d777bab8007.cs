using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using RentalManagement.Models;

namespace RentalManagement.Controllers
{
    public class RentalUnitsController : Controller
    {
        private ApplicationDbContext db = new ApplicationDbContext();

        // GET: RentalUnits
        public ActionResult Index()
        {
            return View(db.RentalUnits.ToList());
        }

        // GET: RentalUnits/Details/5
        public ActionResult Details(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            RentalUnit rentalUnit = db.RentalUnits.Find(id);
            if (rentalUnit == null)
            {
                return HttpNotFound();
            }
            return View(rentalUnit);
        }

        // GET: RentalUnits/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: RentalUnits/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create([Bind(Include = "unitId,rooms,size,address,vacancies")] RentalUnit rentalUnit)
        {
            if (ModelState.IsValid)
            {
                db.RentalUnits.Add(rentalUnit);
                db.SaveChanges();
                return RedirectToAction("Index");
            }

            return View(rentalUnit);
        }

        // GET: RentalUnits/Edit/5
        public ActionResult Edit(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            RentalUnit rentalUnit = db.RentalUnits.Find(id);
            if (rentalUnit == null)
            {
                return HttpNotFound();
            }
            return View(rentalUnit);
        }

        // POST: RentalUnits/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit([Bind(Include = "unitId,rooms,size,address,vacancies")] RentalUnit rentalUnit)
        {
            if (ModelState.IsValid)
            {
                db.Entry(rentalUnit).State = EntityState.Modified;
                db.SaveChanges();
                return RedirectToAction("Index");
            }
            return View(rentalUnit);
        }

        // GET: RentalUnits/Delete/5
        public ActionResult Delete(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            RentalUnit rentalUnit = db.RentalUnits.Find(id);
            if (rentalUnit == null)
            {
                return HttpNotFound();
            }
            return View(rentalUnit);
        }

        // POST: RentalUnits/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public ActionResult DeleteConfirmed(int id)
        {
            RentalUnit rentalUnit = db.RentalUnits.Find(id);
            db.RentalUnits.Remove(rentalUnit);
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