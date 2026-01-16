using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using WhoWantsWhat.Data;
using WhoWantsWhat.Models;
using WhoWantsWhat.Models.ViewModels.ItemsViewModels;

namespace WhoWantsWhat.Controllers
{
    public class ItemsController : Controller
    {
        private readonly ApplicationDbContext _context;
        private readonly UserManager<ApplicationUser> _userManager;

        public ItemsController(ApplicationDbContext context, UserManager<ApplicationUser> userManager)
        {
            _context = context;
            _userManager = userManager;
        }

        private Task<ApplicationUser> GetCurrentUserAsync() => _userManager.GetUserAsync(HttpContext.User);

        // GET: Items
        public async Task<IActionResult> Index()
        {
            var applicationDbContext = _context.Items.Include(i => i.Creator);
            return View(await applicationDbContext.ToListAsync());
        }

        // GET: Items/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var item = await _context.Items
                .Include(i => i.Creator)
                .FirstOrDefaultAsync(m => m.ItemId == id);
            if (item == null)
            {
                return NotFound();
            }

            return View(item);
        }

        // GET: Items/Create
        public async Task<IActionResult> CreateItem(int? WishListId, int? GiftListId)
        {
            var viewModel = new CreateItemViewModel();

            if (WishListId != null)
            {
                viewModel.WishListId = WishListId;
                viewModel.WishList = await _context.WishLists.FindAsync(WishListId);
            }

            if (GiftListId != null)
            {
                viewModel.GiftListId = GiftListId;
                viewModel.GiftList = await _context.GiftLists.FindAsync(GiftListId);
            }

            return View(viewModel);
        }

        // POST: Items/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(CreateItemViewModel viewModel)
        {
            var item = viewModel.Item;

            ModelState.Remove("Item.CreatorId");
            if (ModelState.IsValid)
            {
                var user = await GetCurrentUserAsync();
                item.CreatorId = user.Id;
                item.Purchased = false;
                _context.Items.Add(item);
                await _context.SaveChangesAsync();

                if (viewModel.WishListId != null)
                {
                    var wishListItem = new WishListItem
                    {
                        WishListId = (int)viewModel.WishListId,
                        ItemId = item.ItemId,
                        Notes = viewModel.WishListItem.Notes
                    };
                    _context.WishListItems.Add(wishListItem);
                    await _context.SaveChangesAsync();

                    return RedirectToAction(nameof(Details),"WishLists", new { id = viewModel.WishListId } );
                }
                if (viewModel.GiftListId != null)
                {
                    var giftListItem = new GiftListItem
                    {
                        GiftListId = (int)viewModel.GiftListId,
                        ItemId = item.ItemId,
                        Notes = viewModel.GiftListItem.Notes
                    };
                    _context.GiftListItems.Add(giftListItem);
                    await _context.SaveChangesAsync();

                    return RedirectToAction(nameof(Details), "GiftLists", new { id = viewModel.GiftListId });
                }

            }
            return View(viewModel);
        }

        // GET: Items/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var item = await _context.Items.FindAsync(id);
            if (item == null)
            {
                return NotFound();
            }
            ViewData["CreatorId"] = new SelectList(_context.ApplicationUsers, "Id", "Id", item.CreatorId);
            return View(item);
        }

        // POST: Items/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("ItemId,Name,CreatorId,Link,Purchased,PurchasedAmount")] Item item)
        {
            if (id != item.ItemId)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(item);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!ItemExists(item.ItemId))
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
            ViewData["CreatorId"] = new SelectList(_context.ApplicationUsers, "Id", "Id", item.CreatorId);
            return View(item);
        }

        // GET: Items/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var item = await _context.Items
                .Include(i => i.Creator)
                .FirstOrDefaultAsync(m => m.ItemId == id);
            if (item == null)
            {
                return NotFound();
            }

            return View(item);
        }

        // POST: Items/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var item = await _context.Items.FindAsync(id);
            _context.Items.Remove(item);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool ItemExists(int id)
        {
            return _context.Items.Any(e => e.ItemId == id);
        }

    }
}