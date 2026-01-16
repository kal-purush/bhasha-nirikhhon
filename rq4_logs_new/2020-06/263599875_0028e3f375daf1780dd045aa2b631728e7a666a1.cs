using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using CM.Data;
using CM.Models;
using CM.Services.Contracts;
using CM.DTOs.Mappers.Contracts;
using Microsoft.AspNetCore.Hosting;
using CM.Web.Providers.Contracts;
using NToastNotify;
using CM.Web.Models;
using CM.DTOs;
using CM.Web.Providers;
using CM.Web.Areas.Magician.Models;
using Microsoft.AspNetCore.Authorization;

namespace CM.Web.Controllers
{
    [AllowAnonymous]
    public class CocktailsController : Controller
    {
        private const string ROOTSTORAGE = "\\images\\Cocktails";

        private readonly ICocktailServices _cocktailServices;
        private readonly ICocktailMapper _cocktailMapper;
        private readonly ICocktailViewMapper _cocktailViewMapper;
        private readonly IWebHostEnvironment _webHostEnvironment;
        private readonly IStorageProvider _storageProvider;
        private readonly IRatingServices _ratingServices;
        private readonly IToastNotification _toastNotification;
        private readonly ICommentServices _commentServices;
        private readonly IIngredientServices _ingredientServices;
        private readonly IIngredientViewMapper _ingredientViewMapper;

        public CocktailsController(ICocktailServices cocktailServices,
                                   ICocktailMapper cocktailMapper,
                                   ICocktailViewMapper cocktailViewMapper,
                                   IWebHostEnvironment webHostEnvironment,
                                   IStorageProvider storageProvider,
                                   IRatingServices ratingServices,
                                   IToastNotification toastNotification,
                                   ICommentServices commentServices,
                                   IIngredientServices ingredientServices,
                                   IIngredientViewMapper ingredientViewMapper)
        {
            this._cocktailServices = cocktailServices;
            this._cocktailMapper = cocktailMapper;
            this._cocktailViewMapper = cocktailViewMapper;
            this._webHostEnvironment = webHostEnvironment;
            this._storageProvider = storageProvider;
            this._ratingServices = ratingServices;
            this._toastNotification = toastNotification;
            this._commentServices = commentServices;
            this._ingredientServices = ingredientServices;
            this._ingredientViewMapper = ingredientViewMapper;
        }

        // GET: Magician/Cocktails
        public async Task<IActionResult> Index()
        {
            var permission = User.IsInRole("Magician");
            var dtos = await _cocktailServices.PageCocktailsAsync(allowUnlisted: true);
            var vms = dtos.Select(c => _cocktailViewMapper.CreateCocktailViewModel(c));

            return View(vms);
        }

        [HttpPost]
        [ActionName("Index")]
        public async Task<ActionResult> IndexTable()
        {
            try
            {
                var drawString = HttpContext.Request.Form["draw"].FirstOrDefault();
                int draw = drawString != null ? Convert.ToInt32(drawString) : 0;
                var start = Request.Form["start"].FirstOrDefault();
                var length = Request.Form["length"].FirstOrDefault();
                var searchString = Request.Form["search[value]"].FirstOrDefault();
                var sortBy = Request.Form
                                ["columns[" + Request.Form["order[0][column]"].FirstOrDefault() + "][name]"]
                                .FirstOrDefault();
                var sortOrder = Request.Form["order[0][dir]"].FirstOrDefault();

                int pageSize = length != null ? Convert.ToInt32(length) : 0;
                int pageNumber = start != null ? 1 + (int)Math.Ceiling(Convert.ToDouble(start) / pageSize) : 0;
                int recordsTotal = await _cocktailServices.CountAllCocktailsAsync();

                var dtos = await _cocktailServices.PageCocktailsAsync(searchString, sortBy, sortOrder, pageNumber, pageSize);
                var vms = dtos.Select(d => _cocktailViewMapper.CreateCocktailViewModel(d)).ToList();

                var recordsFiltered = dtos.SourceItems;

                var role = User.IsInRole("Magician") ? "Magician" : "";

                var output = DataTablesProvider<CocktailViewModel>.CreateResponse(draw, recordsTotal, recordsFiltered, role, vms);

                return Ok(output);
            }
            catch (Exception ex)
            {
                _toastNotification.AddErrorToastMessage(ex.Message);
                return RedirectToAction(nameof(Index));
            }
        }

        // GET: Magician/Cocktails/Details/5
        public async Task<IActionResult> Details(Guid id)
        {
            try
            {
                var dto = await _cocktailServices.GetCocktailDetailsAsync(id, isAdmin: false);

                var vm = _cocktailViewMapper.CreateCocktailViewModel(dto);

                // TODO: Comments

                return View(vm);
            }
            catch (Exception ex)
            {
                _toastNotification.AddAlertToastMessage(ex.Message);
                return RedirectToAction(nameof(Index));
            }
        }

        // GET: Magician/Cocktails/Create
        public async Task<IActionResult> Create(CocktailModifyViewModel model)
        {
            var ingredients = await _ingredientServices.GetAllIngredientsAsync();

            var selectListItems = new SelectList(ingredients, nameof(IngredientDTO.Id), nameof(IngredientDTO.Name));

            model.AllIngredients = selectListItems;

            return View(model);
        }

        // POST: Magician/Cocktails/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to, for 
        // more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ActionName("Create")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> CreateConfirmed(CocktailModifyViewModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    if (model.Image != null)
                        model.ImagePath = _storageProvider.GenerateRelativePath(ROOTSTORAGE, model.Image.FileName, model.Name);

                    var dto = _cocktailViewMapper.CreateCocktailDTO(model);
                    dto = await _cocktailServices.CreateCocktailAsync(dto);

                    var vm = _cocktailViewMapper.CreateCocktailViewModel(dto);

                    if (model.Image != null)
                        await _storageProvider.StoreImageAsync(model.ImagePath, model.Image);

                    return RedirectToAction(nameof(Index));
                }
                catch (Exception ex)
                {
                    _toastNotification.AddWarningToastMessage(ex.Message);
                    return RedirectToAction(nameof(Create), model);
                }
            }

            foreach (var item in ModelState.Values)
            {
                foreach (var error in item.Errors)
                {
                    _toastNotification.AddWarningToastMessage(error.ErrorMessage);
                }
            }
            return View(model);
        }

        // GET: Magician/Cocktails/Edit/5
        public async Task<IActionResult> Edit(CocktailModifyViewModel model)
        {
            try
            {
                var dto = await _cocktailServices.GetCocktailDetailsAsync(model.Id);
                model = _cocktailViewMapper.CreateCocktailModifyViewModel(dto);

                var ingredients = await _ingredientServices.GetAllIngredientsAsync();
                var selectListItems = new SelectList(ingredients, nameof(IngredientDTO.Id), nameof(IngredientDTO.Name), dto.Ingredients.ToList());

                model.AllIngredients = selectListItems;

                return View(model);
            }
            catch (Exception ex)
            {
                _toastNotification.AddWarningToastMessage(ex.Message);
                return RedirectToAction(nameof(Index));
            }
        }

        // POST: Magician/Cocktails/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to, for 
        // more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ActionName("Edit")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> EditConfirmed(CocktailModifyViewModel model)
        {
            if (ModelState.IsValid)
            {
                try
                {
                    var oldImagePath = model.ImagePath;
                    if (model.Image != null)
                    {
                        model.ImagePath = _storageProvider.GenerateRelativePath(ROOTSTORAGE, model.Image.FileName, model.Name);
                    }

                    var dto = _cocktailViewMapper.CreateCocktailDTO(model);
                    dto = await _cocktailServices.UpdateCocktailAsync(dto);

                    if (model.Image != null)
                    {
                        _storageProvider.DeleteImage(oldImagePath);
                        await _storageProvider.StoreImageAsync(model.ImagePath, model.Image);
                    }

                    var vm = _cocktailViewMapper.CreateCocktailViewModel(dto);
                    return RedirectToAction(nameof(Index));
                }
                catch (Exception ex)
                {
                    _toastNotification.AddWarningToastMessage(ex.Message);
                    return View(model);
                }
            }

            foreach (var item in ModelState.Values)
            {
                foreach (var error in item.Errors)
                {
                    _toastNotification.AddWarningToastMessage(error.ErrorMessage);
                }
            }
            return View(model);
        }

        // GET: Magician/Cocktails/Delete/5
        public async Task<IActionResult> Delete(Guid id)
        {
            try
            {
                var dto = await _cocktailServices.GetCocktailDetailsAsync(id);
                var vm = _cocktailViewMapper.CreateCocktailViewModel(dto);

                return View(vm);
            }
            catch (Exception ex)
            {
                _toastNotification.AddAlertToastMessage(ex.Message);
                return RedirectToAction(nameof(Index));
            }
        }

        // POST: Magician/Cocktails/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(Guid id)
        {
            try
            {
                var dto = await _cocktailServices.DeleteAsync(id);

                _storageProvider.DeleteImage(dto.ImagePath);

                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                _toastNotification.AddAlertToastMessage(ex.Message);
                return RedirectToAction(nameof(Index));
            }
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> UpdateListing(Guid id, int state)
        {
            try
            {
                var dto = await _cocktailServices.ChangeListingAsync(id, state == 1);
                var vm = _cocktailViewMapper.CreateCocktailViewModel(dto);

                return Ok(vm);
            }
            catch (Exception ex)
            {
                _toastNotification.AddAlertToastMessage(ex.Message);
                return RedirectToAction(nameof(Index));
            }
        }
    }
}