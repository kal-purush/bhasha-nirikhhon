using Microsoft.AspNetCore.Mvc;
using studentprojectapi.Models;
using studentprojectapi.GeneratedModels;
using Microsoft.Identity.Client;
using studentprojectapi.Services;

namespace studentprojectapi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ReportingController : Controller
    {
        private readonly ReportingServices _reportingServices;

        public ReportingController(ReportingServices reportingServices)
        {
            _reportingServices = reportingServices;
        }

        // // show all departments and people that belong to departments

        // GET: ReportingController
        [HttpGet("View Report")]
        public async Task<ActionResult<Array>> GetReport()
        {
            Array returnreport = await _reportingServices.GetReportAsync();

            return Ok(returnreport);

        }

    }
}