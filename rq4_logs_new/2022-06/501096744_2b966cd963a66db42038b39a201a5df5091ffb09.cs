using FoodZone.Services.IServices;
using System.Web.Mvc;

namespace FoodZone.Web.Controllers
{
    public class MenuController : Controller
    {
        private readonly IMenuServices _menuServices;
        private readonly IFoodServices _foodServices;

        public MenuController(IMenuServices menuServices, IFoodServices foodServices)
        {
            _menuServices = menuServices;
            _foodServices = foodServices;
        }

        public ActionResult GetAllMenu()
        {
            var menu = _menuServices.GetAll();
            return PartialView("_GetAllMenu",menu);
        }

        public ActionResult Details(int menuId)
        {
            var menu = _menuServices.GetById(menuId);
            if(menu == null)
            {
                return HttpNotFound();
            }
            ViewBag.Menu = menu.Name;
            var foods = _foodServices.GetFoodByMenu(menuId);
            return View(foods);
        }
    }
}