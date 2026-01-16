using BoldReport.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace BoldReport.Controllers
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
            BoldReports.Models.ReportViewer.ReportDataSource reportDataSource = new BoldReports.Models.ReportViewer.ReportDataSource();
            reportDataSource.Name = "DataSet1";
            reportDataSource.Value = TablaList.GetData();
            ViewBag.dataSources = new List<BoldReports.Models.ReportViewer.ReportDataSource> { reportDataSource };

            return View();
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