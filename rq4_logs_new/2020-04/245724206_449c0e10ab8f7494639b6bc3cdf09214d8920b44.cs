using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using CompetitionApp.Models;
using Microsoft.AspNetCore.Mvc.Rendering;

namespace CompetitionApp.Controllers
{
    public class CategoriesController : Controller
    {
        private readonly ApplicationContext _context;

        public CategoriesController(ApplicationContext context)
        {
            _context = context;
        }
        public async Task<IActionResult> Index()
        {
            return View(await _context.Categories.ToListAsync());
        }

        public async Task<IActionResult> Create()
        {
            List<Category> parentGategories = await _context.Categories.Where(c => c.ParentCategoryId == 0).ToListAsync();
            parentGategories.Insert(0, new Category { Id = 0, Name = "Parent", ParentCategoryId = 0 });
            SelectList  selectParentCategories  = new SelectList(parentGategories, "Id", "Name", 0);

            ViewBag.ParentCategories = selectParentCategories;
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Name, ParentCategoryId")] Category @category)
        {
            if (ModelState.IsValid)
            {
                Category newCategory = new Category { Name = @category.Name, ParentCategoryId = @category.ParentCategoryId };
                _context.Add(newCategory);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));

            }
            return View(@category);
        }

        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var @category= await _context.Categories.FindAsync(id);
            if (@category == null)
            {
                return NotFound();
            }

            List<Category> parentGategories = await _context.Categories.Where(c => c.ParentCategoryId == 0).ToListAsync();
            parentGategories.Insert(0, new Category { Id = 0, Name = "Parent", ParentCategoryId = 0 });
            SelectList selectParentCategories = new SelectList(parentGategories, "Id", "Name", @category.ParentCategoryId);

            ViewBag.ParentCategories = selectParentCategories;
            return View(@category);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("Id,Name, ParentCategoryId")] Category @category)
        {
            if (id != @category.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(@category);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!CategoryExists(@category.Id))
                    {
                        return NotFound();
                    }
                    else
                    {
                        throw;
                    }
                }
                return RedirectToAction(nameof(Index));
            }
            return View(@category);
        }

        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var @category = await _context.Categories.FindAsync(id);
            if (@category == null)
            {
                return NotFound();
            }

            if (@category.ParentCategoryId == 0)
            {
                ViewBag.parentName = "";
            }
            else 
            {
                Category parent = await _context.Categories.FirstOrDefaultAsync(c => c.Id == @category.ParentCategoryId);
                ViewBag.parentName = parent.Name;
            }
                

            return View(@category);
        }

        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var @category = await _context.Categories.FindAsync(id);
            if (@category.ParentCategoryId == 0)
            {
                List<Category> @sub_categories = await _context.Categories.Where(c => c.ParentCategoryId == @category.Id).ToListAsync();
                List<Event> events = new List<Event>();
                foreach (var item in @sub_categories)
                {
                    List<Event> @ev = await _context.Events.Where(e => e.Category.Id == item.Id).ToListAsync();
                    foreach (var it in @ev)
                    {
                        events.Add(it);
                    }
                }
                
                _context.Events.RemoveRange(events);
                _context.Categories.Remove(@category);
                _context.Categories.RemoveRange(@sub_categories);
            }
            else 
            {
                List<Event> @events = await _context.Events.Where(e => e.Category.Id == @category.Id).ToListAsync();
               
                _context.Events.RemoveRange(events);
                _context.Categories.Remove(@category);
            }
            
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool CategoryExists(int id)
        {
            return _context.Categories.Any(c => c.Id == id);
        }
    }
}