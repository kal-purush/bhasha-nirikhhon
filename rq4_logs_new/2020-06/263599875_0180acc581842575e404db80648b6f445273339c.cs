using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CM.DTOs.Mappers.Contracts;
using CM.Services.Contracts;
using CM.Web.Providers.Contracts;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CM.Web.Areas.Magician.Controllers
{
    public class CocktailsController : Controller
    {
        private readonly ICocktailServices _cocktailServices;
        private readonly ICocktailMapper _cocktailMapper;
        private readonly IWebHostEnvironment _webHostEnvironment;
        private readonly IStorageProvider _storageProvider;

        public CocktailsController(ICocktailServices cocktailServices,
                                   ICocktailMapper cocktailMapper,
                                   IWebHostEnvironment webHostEnvironment,
                                   IStorageProvider storageProvider)
        {
            this._cocktailServices = cocktailServices;
            this._cocktailMapper = cocktailMapper;
            this._webHostEnvironment = webHostEnvironment;
            this._storageProvider = storageProvider;
        }

        // GET: CocktailsController
        public ActionResult Index()
        {
            return View();
        }

        // GET: CocktailsController/Details/5
        public ActionResult Details(int id)
        {
            return View();
        }

        // GET: CocktailsController/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: CocktailsController/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(IFormCollection collection)
        {
            try
            {
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: CocktailsController/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: CocktailsController/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit(int id, IFormCollection collection)
        {
            try
            {
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: CocktailsController/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: CocktailsController/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(int id, IFormCollection collection)
        {
            try
            {
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }
    }
}