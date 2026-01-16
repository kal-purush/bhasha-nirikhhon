using System;
using System.Collections.Generic;
using System.Data;
//using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using System.Web;
//using System.Web.Mvc;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ZenithSocietyA2.Models;
using ZenithSocietyA2.Data;

namespace ZenithWebSite.Controllers
{
    [Authorize(Roles = "Admin")]
    public class ActivityCategoriesController : Controller
    {
        private ApplicationDbContext db;

        public ActivityCategoriesController(ApplicationDbContext context)
        {
            db = context;
        }
        
        
        // GET: ActivityCategories
        public async Task<ActionResult> Index()
        {
            return View(await db.ActivityCategories.ToListAsync());
        }

        // GET: ActivityCategories/Details/5
        public async Task<ActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }
            ActivityCategory activityCategory = await db.ActivityCategories.FindAsync(id);
            if (activityCategory == null)
            {
                return NotFound();
            }
            return View(activityCategory);
        }

        // GET: ActivityCategories/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: ActivityCategories/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see https://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Create([Bind("ActivityCategoryId,ActivityDescription,CreationDate")] ActivityCategory activityCategory)
        {
            activityCategory.CreationDate = DateTime.Now;
            if (ModelState.IsValid)
            { 
                db.ActivityCategories.Add(activityCategory);
                await db.SaveChangesAsync();
                return RedirectToAction("Index");
            }

            return View(activityCategory);
        }

        // GET: ActivityCategories/Edit/5
        public async Task<ActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }
            ActivityCategory activityCategory = await db.ActivityCategories.FindAsync(id);
            if (activityCategory == null)
            {
                return NotFound();
            }
            return View(activityCategory);
        }

        // POST: ActivityCategories/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see https://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Edit([Bind("ActivityCategoryId,ActivityDescription,CreationDate")] ActivityCategory activityCategory)
        {
            if (ModelState.IsValid)
            {
                db.Entry(activityCategory).State = EntityState.Modified;
                await db.SaveChangesAsync();
                return RedirectToAction("Index");
            }
            return View(activityCategory);
        }

        // GET: ActivityCategories/Delete/5
        public async Task<ActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }
            ActivityCategory activityCategory = await db.ActivityCategories.FindAsync(id);
            if (activityCategory == null)
            {
                return NotFound();
            }
            return View(activityCategory);
        }

        // POST: ActivityCategories/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> DeleteConfirmed(int id)
        {
            ActivityCategory activityCategory = await db.ActivityCategories.FindAsync(id);
            db.ActivityCategories.Remove(activityCategory);
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