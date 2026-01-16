using AnagramSolver.BusinessLogic.Classes;
using AnagramSolver.WebApp.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AnagramSolver.WebApp.Controllers
{
    public class AnagramController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private const string _filePath = "C:\\Users\\jonas.jaugelis\\source\\repos\\AnagramSolver\\zodynas.txt";

        public AnagramController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            var _wordRepository = new WordRepository();
            var words = _wordRepository.GetSpecificAmountOfWords(_filePath);
            HashSet<AnagramViewModel> newList = new HashSet<AnagramViewModel>();
            foreach (var word in words)
            {
                var anagram = new AnagramViewModel();
                anagram.Word = word;
                newList.Add(anagram);
            }
            ViewBag.newList = newList;
            return View();
        }
    }
}