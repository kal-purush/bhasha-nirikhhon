using Microsoft.AspNetCore.Mvc;
using ModelBindingStyling_Lab.Models;
using System.Diagnostics;

namespace ModelBindingStyling_Lab.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult UserProfile()
        {
            UserProfile user = GetUserProfileData();
            return View(user);
        }

        public IActionResult PrinterList()
        {
            IEnumerable<ThreeDPrinters> printers = GetPrinterList();
            return View(printers);
        }

        /// <summary>
        /// Returns a list of 3d printers with test data for use
        /// on a view to practice modelbinding
        /// </summary>
        /// <returns></returns>
        private IEnumerable<ThreeDPrinters> GetPrinterList()
        {
            return new List<ThreeDPrinters>()
            {
                new ThreeDPrinters 
                {
                    BuildVolume = "180 x 180 x 180 mm",
                    Price = 499.99,
                    SKU = "SMLPRINTER7",
                    Title = "The mini printer"
                },
                new ThreeDPrinters 
                {
                    BuildVolume = "300 x 300 x 300 mm",
                    Price = 899.99,
                    SKU = "MEDPRINTER12",
                    Title = "Mega Printer"
                },
                new ThreeDPrinters 
                {
                    BuildVolume = "360 x 360 x 360 mm",
                    Price = 999.99,
                    SKU = "MEDPRINTER12P",
                    Title = "Mega Printer Plus"
                },
                new ThreeDPrinters 
                {
                    BuildVolume = "360 x 360 x 500 mm",
                    Price = 899.99,
                    SKU = "GIGANTOR10",
                    Title = "Mega Printer"
                }
            };
        }

        /// <summary>
        /// This method returns hardcoded data for a UserProfile
        /// to use with modelbinding on a view
        /// </summary>
        private UserProfile GetUserProfileData()
        {
            return new UserProfile()
            {
                DateOfBirth = new DateOnly(1815, 7, 1),
                Email = "First.Programmer@gmail.com",
                FullName = "Ada Lovelace",
                GitHubUrl = "https://github.com/Octocat",
                ImageUrl = "https://placehold.co/150",
                PhoneNumber = "(253) 555-1234",
                UserProfileId = 10,
                SkilledLanguages = new List<string> { "C#", "Java", "C++" }
            };
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}