using HackathonWeb.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Diagnostics;

namespace HackathonWeb.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly Config _config; 

        public HomeController(ILogger<HomeController> logger,
            IOptions<Config> options)
        {
            _config = options.Value;
        }

        public IActionResult Index()
        {
            return View(new HomeViewModel { WeatherForecastList = new List<WeatherForecast>() });
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

        [HttpGet]
        public async Task<IActionResult> GetWeather()
        {
            var result = new List<WeatherForecast>();
            var url = $"{_config.HackathonApiUrl}/WeatherForecast/get";
            try {
                HttpResponseMessage response = await new HttpClient().GetAsync(url);
                if (response.IsSuccessStatusCode)
                {
                    result = await response.Content.ReadFromJsonAsync<List<WeatherForecast>>();
                }
            }
            catch (Exception ex)
            {

            }

            HomeViewModel viewModel = new HomeViewModel { WeatherForecastList = result };
            return View("Index", viewModel);
        }

        [HttpGet]
        public async Task<IActionResult> Index(string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return Index();
            }

            var result = string.Empty;
            var url = $"{_config.HackathonApiUrl}/String/ReverseString?inputString={input}";
            try
            {
                HttpResponseMessage response = await new HttpClient().GetAsync(url);
                if (response.IsSuccessStatusCode)
                {
                    result = await response.Content.ReadFromJsonAsync<string>();
                }
            }
            catch (Exception ex)
            {

            }

            HomeViewModel viewModel = new HomeViewModel { ReversedString = result, WeatherForecastList = new List<WeatherForecast>() };
            return View("Index", viewModel);
        }
    }
}