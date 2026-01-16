using System;
using System.Collections.Generic;
using System.Data;
//using System.Data.Entity;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using ZenithSocietyA2.Data;
//using System.Web.Mvc;
using ZenithSocietyA2.Models;

namespace ZenithSocietyA2.Controllers
{
    [Authorize(Roles = "Admin")]
    public class EventsController : Controller
    {
        private ApplicationDbContext db;
        
        public EventsController(ApplicationDbContext context)
        {
            db = context;
        }

        // GET: Events
        public async Task<IActionResult> Index()
        {
            var events = db.Events.Include(x => x.ActivityCategory);
            
            return View(await events.ToListAsync());
        }

      
        // GET: Events/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }
            Event @event = await db.Events.Include( x => x.ActivityCategory ).SingleOrDefaultAsync(x => x.EventId == id);
            if (@event == null)
            {
                return NotFound();
            }
            return View(@event);
        }

        // GET: Events/Create
        public IActionResult Create()
        {
            ViewBag.ActivityCategoryId = new SelectList(db.ActivityCategories, "ActivityCategoryId", "ActivityDescription");
            return View();
        }

        // POST: Events/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see https://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("EventId,FromDate,ToDate,ActivityCategoryId,IsActive,CreationDate,EnteredByUsername")] Event @event)
        {
            @event.EnteredByUsername = User.Identity.Name;
            @event.CreationDate = DateTime.Now;

            if (ModelState.IsValid)
            {
                db.Events.Add(@event);
                await db.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }


            ViewBag.ActivityCategoryId = new SelectList(db.ActivityCategories, "ActivityCategoryId", "ActivityDescription", @event.ActivityCategoryId);
            return View(@event);
        }

        // GET: Events/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return StatusCode(StatusCodes.Status400BadRequest);
            }
            Event @event = await db.Events.SingleOrDefaultAsync(m => m.EventId == id);
            if (@event == null)
            {
                return NotFound();
            }
            ViewBag.ActivityCategoryId = new SelectList(db.ActivityCategories, "ActivityCategoryId", "ActivityDescription", @event.ActivityCategoryId);
            return View(@event);
        }

        // POST: Events/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see https://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit([Bind("EventId,FromDate,ToDate,EnteredByUsername,ActivityCategoryId,CreationDate,IsActive")] Event @event)
        {
            if (ModelState.IsValid)
            {
                db.Entry(@event).State = EntityState.Modified;
                await db.SaveChangesAsync();
                return RedirectToAction("Index");
            }
            ViewBag.ActivityCategoryId = new SelectList(db.ActivityCategories, "ActivityCategoryId", "ActivityDescription", @event.ActivityCategoryId);
            return View(@event);
        }

        // GET: Events/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }
            Event @event = await db.Events.SingleOrDefaultAsync(m => m.EventId == id);
            if (@event == null)
            {
                return NotFound();
            }
            return View(@event);
        }

        // POST: Events/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            Event @event = db.Events.Find(id);
            db.Events.Remove(@event);
            await db.SaveChangesAsync();
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