// -----------------------------------------------------------------------
// <copyright file="ManageController.cs" company="1-system-group">
// Copyright (c) 1-system-group. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Diary_Sample.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Diary_Sample.Controllers
{
    [Authorize]
    public class ManageController : Controller
    {
        private readonly UserManager<IdentityUser> _userManager;
        private readonly ILogger<ManageController> _logger;
        public ManageController(
            ILogger<ManageController> logger,
            UserManager<IdentityUser> userManager)
        {
            _userManager = userManager;
            _logger = logger;
        }
        [HttpGet]
        public IActionResult Index()
        {
            return View(new ManageViewModel(_userManager, 1));
        }
        [HttpGet]
        public IActionResult Paging(int page)
        {
            return View("Index", new ManageViewModel(_userManager, page));
        }
        [HttpPost]
        public async Task<IActionResult> UnlockAsync(string unlockId)
        {
            IdentityUser user = await _userManager.FindByIdAsync(unlockId).ConfigureAwait(false);
            // ロック解除
            await _userManager.SetLockoutEndDateAsync(user, null).ConfigureAwait(false);
            // ロックを解除したユーザのページを表示
            return View("Index", new ManageViewModel(_userManager, user));
        }
    }
}