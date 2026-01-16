using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Gride.Data;
using Gride.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Gride.Controllers
{
    public class MessageController : Controller { 

        private readonly ApplicationDbContext _context;

        public MessageController(ApplicationDbContext context)
        {
            _context = context;
        }

        // GET: Function
        public async Task<IActionResult> Index()
        {
            var messages = _context.Messages
                .Include(m => m.Employee)
                .Include(c => c.Comments)
                    .ThenInclude(e => e.Employee)
                .AsNoTracking();

            messages = messages.OrderByDescending(e => e.Time);

            messages.ToList();

            return View(messages);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Comment(int id)
        {
            Comment comment = new Comment
            {
                Text = Request.Form["comment"],
                Employee = _context.EmployeeModel
                .Single(e => e.EMail == User.Identity.Name)
            };
            _context.Add(comment);
            _context.SaveChanges();

            Message message = _context.Messages.Single(m => m.MessageID == id);
            if (message.Comments == null)
            {
                var comments = new Comment[]
                {
                    comment
                };
                message.Comments = comments;
            } else
            {
                message.Comments.Add(comment);
            }
            await _context.SaveChangesAsync();

            return RedirectToAction(nameof(Index));
        }

        public IActionResult OnGetPartial() =>
        new PartialViewResult
        {
            ViewName = "~/Views/Comment/_AddComment.cshtml",
            ViewData = ViewData,
        };

        // GET: Function/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var message = await _context.Messages
                .FirstOrDefaultAsync(m => m.MessageID == id);
            if (message == null)
            {
                return NotFound();
            }

            return View(message);
        }

        // GET: Function/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: Function/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Text")] Message message )
        {
            var employee = _context.EmployeeModel
                .Single(e => e.EMail == User.Identity.Name);

            message.Employee = employee;

            if (ModelState.IsValid)
            {
                _context.Add(message);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }

            return View(message);
        }

        // GET: Function/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var message = await _context.Messages.FindAsync(id);
            if (message == null)
            {
                return NotFound();
            }
            return View(message);
        }

        // POST: Function/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("MessageID, Text")] Message message)
        {
            if (id != message.MessageID)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(message);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!MessageExists(message.MessageID))
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
            return View(message);
        }

        // GET: Function/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var message = await _context.Messages
                .FirstOrDefaultAsync(m => m.MessageID == id);
            if (message == null)
            {
                return NotFound();
            }

            return View(message);
        }

        // POST: Function/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var message = await _context.Messages.FindAsync(id);
            _context.Messages.Remove(message);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool MessageExists(int id)
        {
            return _context.Messages.Any(e => e.MessageID == id);
        }
    }
}