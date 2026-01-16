using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using WhoWantsWhat.Data;
using WhoWantsWhat.Models;
using WhoWantsWhat.Models.ViewModels.ReportsViewModels;

namespace WhoWantsWhat.Controllers
{
    [Authorize]
    public class ReportsController : Controller
    {
        private readonly ApplicationDbContext _context;
        private readonly UserManager<ApplicationUser> _userManager;

        public ReportsController(ApplicationDbContext context, UserManager<ApplicationUser> userManager)
        {
            _context = context;
            _userManager = userManager;
        }
        private Task<ApplicationUser> GetCurrentUserAsync() => _userManager.GetUserAsync(HttpContext.User);

        public async Task<IActionResult> Index()
        {
            var viewModel = new ListTypeViewModel
            {
                ListTypes = await _context.ListTypes.ToListAsync()
            };
            return View(viewModel);
        }

        public async Task<IActionResult> SpendingReport(int ListTypeId)
        {
            var user = await GetCurrentUserAsync();

            var giftLists = await _context.GiftLists
                .Include(gl => gl.GiftListItems)
                .ThenInclude(gli => gli.Item)
                .Where(gl => gl.CreatorId == user.Id && gl.ListTypeId == ListTypeId)
                .ToListAsync();

            foreach(var gl in giftLists)
            {
                gl.AmountSpent = gl.GiftListItems.Select(gli => gli.Item.PurchasedAmount).Sum();
            }

            var viewModel = new BudgetReportViewModel
            {
                ListTypeId = ListTypeId,
                ListType = await _context.ListTypes.FindAsync(ListTypeId),
                GiftLists = giftLists,
                TotalAmountSpent = giftLists.Select(gl => gl.AmountSpent).Sum(),
                TotalBudget = giftLists.Select(gl => gl.Budget).Sum()
            };

            return View(viewModel);
        }
    }
}