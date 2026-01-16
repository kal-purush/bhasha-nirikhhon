using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using PlaneLog.Models;

namespace PlaneLog.Controllers
{
    public class SquawksController : Controller
    {
        private ApplicationDbContext db = new ApplicationDbContext();

        // GET: Squawks
        public ActionResult Index()
        {
            return View(db.Squawks.ToList());
        }

        // GET: Squawks/Details/5
        public ActionResult Details(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Squawk squawk = db.Squawks.Find(id);
            if (squawk == null)
            {
                return HttpNotFound();
            }
            return View(squawk);
        }

        // GET: Squawks/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Squawks/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create([Bind(Include = "Id,PlaneId,Issue,DateRercorded,Resolved,DateResolved,WhoResolved,CostToResolve,ContactPhone,ContactName")] Squawk squawk)
        {
            if (ModelState.IsValid)
            {
                db.Squawks.Add(squawk);
                db.SaveChanges();
                return RedirectToAction("Index");
            }

            return View(squawk);
        }

        // GET: Squawks/Edit/5
        public ActionResult Edit(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Squawk squawk = db.Squawks.Find(id);
            if (squawk == null)
            {
                return HttpNotFound();
            }
            return View(squawk);
        }

        // POST: Squawks/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit([Bind(Include = "Id,PlaneId,Issue,DateRercorded,Resolved,DateResolved,WhoResolved,CostToResolve,ContactPhone,ContactName")] Squawk squawk)
        {
            if (ModelState.IsValid)
            {
                db.Entry(squawk).State = EntityState.Modified;
                db.SaveChanges();
                return RedirectToAction("Index");
            }
            return View(squawk);
        }

        // GET: Squawks/Delete/5
        public ActionResult Delete(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Squawk squawk = db.Squawks.Find(id);
            if (squawk == null)
            {
                return HttpNotFound();
            }
            return View(squawk);
        }

        // POST: Squawks/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public ActionResult DeleteConfirmed(int id)
        {
            Squawk squawk = db.Squawks.Find(id);
            db.Squawks.Remove(squawk);
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