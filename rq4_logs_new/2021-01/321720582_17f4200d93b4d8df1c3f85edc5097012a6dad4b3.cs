using ehpad.ORM;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Threading.Tasks;

namespace ehpad.WEB.Controllers
{
    public class FilterController : Controller
    {

        private readonly Context _context = new Context();

        // GET: FilterController
        public ActionResult Index()
        {
            IEnumerable<SelectListItem> selectList = from p in _context.Peoples
                                                     select new SelectListItem
                                                     {
                                                         Value = p.Id.ToString(),
                                                         Text = p.Name + " " + p.Firstname
                                                     };
            ViewData["People"] = new SelectList(
               selectList, 
                "Value", 
                "Text");
            ViewData["Injection"] = null;
            return View();

        }

        // POST: Filter/Details
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Details([Bind("Id")] People people)
        {
            IEnumerable<SelectListItem> selectList = from p in _context.Peoples
                                                     select new SelectListItem
                                                     {
                                                         Value = p.Id.ToString(),
                                                         Text = p.Name + " " + p.Firstname
                                                     };
            ViewData["People"] = new SelectList(
               selectList,
                "Value",
                "Text");

            ViewData["Injection"] = await _context.Injections
                .Include("Vaccine.Drug")
                .Where(m => m.PeopleId == people.Id).ToListAsync();

            return View("Index");
        }

    }
}