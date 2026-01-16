using FoodZone.Services.IServices;
using FoodZone.Web.Areas.Admin.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using FoodZone.Models.Common;
using Microsoft.AspNetCore.Mvc;

namespace FoodZone.Web.Areas.Admin.Controllers
{
    //[Authorize(Roles = "Owner")]
    //[Authorize(Roles = "Manager")]
    [Area("Admin")]
    public class MenuManagementController : Controller
    {
        private readonly IMenuServices _menuServices;

        public MenuManagementController(IMenuServices menuServices)
        {
            _menuServices = menuServices;
        }

        // GET: MenuManagementController
        public ActionResult Index()
        {
            var menu = _menuServices.GetAll();
            return View(menu);
        }

        // GET: MenuManagementController/Create
        public ActionResult Create()
        {
            var menuViewModel = new MenuViewModel();
            return View(menuViewModel);
        }

        // POST: MenuManagementController/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Create(MenuViewModel model)
        {
            if (ModelState.IsValid)
            {
                var menu = new Menu
                {
                    Name = model.Name,
                    Description = model.Description
                };

                var result = await _menuServices.AddAsync(menu);
                if (result > 0)
                {
                    TempData["Message"] = "Tạo thành công.";
                    return RedirectToAction("Index");
                }
                else
                {
                    TempData["Message"] = "Tạo thất bại. Thử lại sau nhé!";
                }
                return RedirectToAction("Index");
            }
            return View(model);
        }

        // GET: MenuManagementController/Edit/5
        public async Task<ActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return new BadRequestResult();
            }

            var menu = await _menuServices.GetByIdAsync((int)id);
            if(menu == null)
            {
                return NotFound();
            }

            var menuViewModel = new MenuViewModel
            {
                Name = menu.Name,
                Description = menu.Description
            };

            return View(menuViewModel);
        }

        // POST: MenuManagementController/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Edit(int? id, MenuViewModel menuViewModel)
        {
            if (ModelState.IsValid)
            {
                var menu = await _menuServices.GetByIdAsync(menuViewModel.Id);
                if(menu == null)
                {
                    return NotFound();
                }

                menu.Name = menuViewModel.Name;
                menu.Description = menuViewModel.Description;

                var result = await _menuServices.UpdateAsync(menu);
                if (result)
                {
                    TempData["Message"] = "Cập nhật thành công.";
                }
                else
                {
                    TempData["Message"] = "Cập nhật thất bại.";
                }
                return RedirectToAction("Index");
            }
            return View(menuViewModel);
        }

        [HttpPost, ActionName("Delete")]
        public async Task<ActionResult> Delete(int id)
        {
            var result = await _menuServices.DeleteAsync(id);
            if (result)
            {
                TempData["Message"] = "Xóa thành công.";
            }
            else
            {
                TempData["Message"] = "Xóa thất bại!";
            }
            return RedirectToAction("Index");
        }
    }
}