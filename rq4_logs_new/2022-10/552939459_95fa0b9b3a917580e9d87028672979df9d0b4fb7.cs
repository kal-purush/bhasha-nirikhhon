using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using Vacancy.Repository.Repositories;
using Vakansion.Models;

namespace Vakansion.UI.Controllers
{


    public class DataController : Controller
    {
        private readonly ILogger<DataController> _logger;
        private readonly DataRepository _dataRepository;
        public DataController(ILogger<DataController> logger, DataRepository dataRepository)
        {
            _logger = logger;
            _dataRepository = dataRepository;
        }

        public async Task<IActionResult> Index()
        {
            return View(await _dataRepository.GetDataAsync());
        }


        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

    }
}