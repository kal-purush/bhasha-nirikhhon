using EasyTravel.Domain.Services;
using EasyTravel.Web.Models;
using Microsoft.AspNetCore.Mvc;

namespace EasyTravel.Web.Controllers
{
    public class CarSearchController : Controller
    {
        private ISessionService _sessionService;
        private IBusService _busService;

        public CarSearchController(IBusService busservice, ISessionService sessionService)
        {
            _sessionService = sessionService;
            _busService = busservice;
        }



        [HttpGet]
        public IActionResult Index()
        {
            return View();
        }


        [HttpPost, ValidateAntiForgeryToken]
        public IActionResult Index(BusSearchFormModel model)
        {
            if (ModelState.IsValid)
            {
                // Storing search parameters in session
                _sessionService.SetString("From", model.From);
                _sessionService.SetString("To", model.To);
                _sessionService.SetString("DateTime", model.DepartureTime.ToString());

                // Redirect to the List method of BusController
                return RedirectToAction("List", "Car");
            }

            return View(model);
        }
    }





      
    }
