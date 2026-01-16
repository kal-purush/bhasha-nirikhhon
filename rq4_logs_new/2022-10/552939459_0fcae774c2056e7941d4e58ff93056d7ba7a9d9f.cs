using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using Vacancy.Repository.Repositories;
using Vakansion.Models;

namespace Vakansion.Controllers
{
    public class VacancyController : Controller
    {
            private readonly ILogger<VacancyController> _logger;
            private readonly VacancyRepository _vacancyRepository;
            public VacancyController(ILogger<VacancyController> logger, VacancyRepository vacancyRepository)
            {
                _logger = logger;
            _vacancyRepository = vacancyRepository;
            }

            public async Task<IActionResult> Index()
            {
                return View(await _vacancyRepository.GetVacancyAsync());
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