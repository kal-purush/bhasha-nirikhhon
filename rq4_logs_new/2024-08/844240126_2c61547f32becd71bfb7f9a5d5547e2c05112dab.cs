using BoreholeData;
using BoreholeData.Repositories;
using BoreholeMVCUI.Models;
using Microsoft.AspNetCore.Mvc;

namespace BoreholeMVCUI.Controllers
{
    public class BoreholeController : Controller
    {
        private BoreholeRepository repository;

        public BoreholeController(BoreholeContext context)
        {
            repository = new BoreholeRepository(context);
        }

        public IActionResult Index()
        {
            var allBoreholes = repository.GetAll().Select(bh => new BoreholeViewModel() { Id = bh.ID, X = bh.X, Y = bh.Y, Depth = bh.Depth }).ToList();

            return View(allBoreholes);
        }
    }
}