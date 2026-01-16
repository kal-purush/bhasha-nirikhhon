using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Stock.Management.Api.Controllers
{
    public class CustomerInvestmentController : Controller
    {
        // GET: CustomerInvestmentController
        public ActionResult Index()
        {
            return View();
        }

        // GET: CustomerInvestmentController/Details/5
        public ActionResult Details(int id)
        {
            return View();
        }

        // GET: CustomerInvestmentController/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: CustomerInvestmentController/Create
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

        // GET: CustomerInvestmentController/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: CustomerInvestmentController/Edit/5
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

        // GET: CustomerInvestmentController/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: CustomerInvestmentController/Delete/5
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