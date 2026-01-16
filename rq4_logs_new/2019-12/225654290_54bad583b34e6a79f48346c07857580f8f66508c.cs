using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using WhoWantsWhat.Data;
using WhoWantsWhat.Models;
using WhoWantsWhat.Models.ViewModels.GiftListsViewModels;

namespace WhoWantsWhat.Controllers
{
    [Authorize]
    public class GiftListsController : Controller
    {
        private readonly ApplicationDbContext _context;
        private readonly UserManager<ApplicationUser> _userManager;

        public GiftListsController(ApplicationDbContext context, UserManager<ApplicationUser> userManager)
        {
            _context = context;
            _userManager = userManager;
        }
        private Task<ApplicationUser> GetCurrentUserAsync() => _userManager.GetUserAsync(HttpContext.User);

        // GET: GiftLists
        public async Task<IActionResult> Index()
        {
            var user = await GetCurrentUserAsync();
            var giftLists = await _context.GiftLists.Where(g => g.Creator == user).Include(g => g.ListType).Include(g => g.Receiver).ToListAsync();
            return View(giftLists);
        }

        // GET: GiftLists/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var giftList = await _context.GiftLists
                .Include(g => g.ListType)
                .Include(g => g.Receiver)
                .Include(g => g.GiftListItems)
                .ThenInclude(gli => gli.Item)
                .FirstOrDefaultAsync(m => m.GiftListId == id);
            if (giftList == null)
            {
                return NotFound();
            }

            return View(giftList);
        }

        // GET: GiftLists/Create
        public async Task<IActionResult> Create()
        {
            var user = await GetCurrentUserAsync();
            var viewModel = new CreateGiftListViewModel
            {
                Receivers = await _context.ApplicationUsers
                    .Include(u => u.GroupUsers)
                    .Where(u => u.Id != user.Id)
                    // filter to include users who share a group with the logged in user
                    .Where(u => u.GroupUsers
                        .Any(gu => gu.Group.GroupUsers
                            .Any(gu => gu.UserId == user.Id)))
                    .ToListAsync(),
                ListTypes = await _context.ListTypes.ToListAsync()
            };
            return View(viewModel);
        }

        // POST: GiftLists/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("GiftListId,Name,CreatorId,ReceiverId,ReceiverName,ListTypeId,DateNeeded,Budget")] GiftList giftList)
        {
            if (ModelState.IsValid)
            {
                _context.Add(giftList);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            ViewData["CreatorId"] = new SelectList(_context.ApplicationUsers, "Id", "Id", giftList.CreatorId);
            ViewData["ListTypeId"] = new SelectList(_context.ListTypes, "ListTypeId", "Label", giftList.ListTypeId);
            ViewData["ReceiverId"] = new SelectList(_context.ApplicationUsers, "Id", "Id", giftList.ReceiverId);
            return View(giftList);
        }

        // GET: GiftLists/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var giftList = await _context.GiftLists.FindAsync(id);
            if (giftList == null)
            {
                return NotFound();
            }
            ViewData["CreatorId"] = new SelectList(_context.ApplicationUsers, "Id", "Id", giftList.CreatorId);
            ViewData["ListTypeId"] = new SelectList(_context.ListTypes, "ListTypeId", "Label", giftList.ListTypeId);
            ViewData["ReceiverId"] = new SelectList(_context.ApplicationUsers, "Id", "Id", giftList.ReceiverId);
            return View(giftList);
        }

        // POST: GiftLists/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("GiftListId,Name,CreatorId,ReceiverId,ReceiverName,ListTypeId,DateNeeded,Budget")] GiftList giftList)
        {
            if (id != giftList.GiftListId)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(giftList);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!GiftListExists(giftList.GiftListId))
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
            ViewData["CreatorId"] = new SelectList(_context.ApplicationUsers, "Id", "Id", giftList.CreatorId);
            ViewData["ListTypeId"] = new SelectList(_context.ListTypes, "ListTypeId", "Label", giftList.ListTypeId);
            ViewData["ReceiverId"] = new SelectList(_context.ApplicationUsers, "Id", "Id", giftList.ReceiverId);
            return View(giftList);
        }

        // GET: GiftLists/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var giftList = await _context.GiftLists
                .Include(g => g.Creator)
                .Include(g => g.ListType)
                .Include(g => g.Receiver)
                .FirstOrDefaultAsync(m => m.GiftListId == id);
            if (giftList == null)
            {
                return NotFound();
            }

            return View(giftList);
        }

        // POST: GiftLists/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var giftList = await _context.GiftLists.FindAsync(id);
            _context.GiftLists.Remove(giftList);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool GiftListExists(int id)
        {
            return _context.GiftLists.Any(e => e.GiftListId == id);
        }
    }
}