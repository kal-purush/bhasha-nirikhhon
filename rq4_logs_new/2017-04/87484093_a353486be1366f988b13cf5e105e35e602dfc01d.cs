using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.Entity;
using System.Net;
using System.Web.Mvc;
using RentalManagement.Models;
using RentalManagement.ViewModel;

namespace RentalManagement.Controllers {
    public class ApprovementController : Controller {

        private ApplicationDbContext db = new ApplicationDbContext();

        public ActionResult Index() {
            var clients = db.Clients.Include(c => c.OccupancyRecords).Include(c => c.HomeAddress)
                          .Include(c => c.WorkAddress);
            return View(clients.ToList());
        }

        public ActionResult Approve(int? id) {
            if (id == null) {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }

            Client client = db.Clients.Find(id);

            if (client != null) {
                foreach (var record in client.OccupancyRecords) {
                    record.StartDate = DateTime.Today;
                    record.EndDate = DateTime.Today.AddYears(1);

                    db.OccupancyRecords.Add(record);
                };

                db.SaveChanges();
            }


            return RedirectToAction("Index", "Approvement");

        }

        public ActionResult Deny(int? id) {
            if (id == null) {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Client client = db.Clients.Find(id);
            if (client == null) {
                return HttpNotFound();
            }
            return View(client);
        }

        public ActionResult Details(int id) {
            var clients = db.Clients.Include(c => c.OccupancyRecords).Include(c => c.HomeAddress).Include(c => c.WorkAddress).SingleOrDefault(c => c.Id == id);
            if (clients == null) {
                return HttpNotFound();
            }

            return View(clients);
        }
    }
}