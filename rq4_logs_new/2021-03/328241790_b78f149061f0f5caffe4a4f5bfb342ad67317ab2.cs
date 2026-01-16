using CPW217_PortfolioProject2021.Data;
using CPW217_PortfolioProject2021.Models;
using FileUploadExample.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CPW217_PortfolioProject2021.Controllers
{
	public class ItemsController : Controller
	{
		private readonly ApplicationDbContext _context;
		private readonly IConfiguration _config;

		public ItemsController(IConfiguration config, ApplicationDbContext context)
		{
			_context = context;
			_config = config;
		}
		public async Task<IActionResult> Index()
		{
			return View(await _context.Items.ToListAsync());
		}

		public IActionResult Create()
		{
			return View();
		}

        [HttpPost]
        [ValidateAntiForgeryToken]
        [RequestSizeLimit(8388608)] // 8MB
        public async Task<IActionResult> Create(Item item)
        {
            if (ModelState.IsValid)
            {
                IFormFile photo = item.Photo;

                if (photo.Length > 0)
                {
                    // Add error message
                    // return view
                }

                if (!FileUploadHelper.IsValidExtension(photo, FileUploadHelper.FileTypes.Photo))
                {
                    // Add error message
                    // return view
                }

                BlobStorageHelper helper = new BlobStorageHelper(_config);
                item.PhotoUrl = await helper.UploadBlob(photo);

                _context.Add(item);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            return View(item);
        }
    }
}