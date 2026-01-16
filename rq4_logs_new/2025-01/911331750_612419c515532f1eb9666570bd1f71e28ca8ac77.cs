using Microsoft.AspNetCore.Mvc;
using AppVecinos.Web.Services;

namespace AppVecinos.Web.Controllers
{
    public class NeighborsController : Controller
    {
        private readonly INeighborService _neighborService;
        private readonly ILogger<NeighborsController> _logger;

        public NeighborsController(INeighborService neighborService, ILogger<NeighborsController> logger)
        {
            _neighborService = neighborService;
            _logger = logger;
        }
        // GET: NeighborsController
        public async Task<ActionResult> Index()
        {
            var token = HttpContext.Session.GetString("AuthToken");
            Console.WriteLine($"Token session: {token}");

            var neighbors = await _neighborService.GetNeighborsAsync(token);
            return View(neighbors);
        }

        // GET: NeighborsController/Details/5
        public ActionResult Details(int id)
        {
            return View();
        }

        // GET: NeighborsController/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: NeighborsController/Create
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

        // GET: NeighborsController/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: NeighborsController/Edit/5
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

        // GET: NeighborsController/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: NeighborsController/Delete/5
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