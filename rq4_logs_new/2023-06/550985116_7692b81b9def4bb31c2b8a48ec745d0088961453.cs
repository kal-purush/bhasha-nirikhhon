using Microsoft.AspNetCore.Mvc;
using System.Text;
using System;
using AgravitaeWebExtension.Helper;
using AgravitaeWebExtension.Services;
using Microsoft.Extensions.Logging;
using AgravitaeWebExtension.Models.Client_Requests;
using AgravitaeWebExtension.Models.GenericReports;

namespace AgravitaeWebExtension.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class GenericReportController : ControllerBase
    {
        private readonly IGenericReportService _genericReportService;
        private readonly ILogger<GenericReportController> _logger;
        public GenericReportController(IGenericReportService genericReportService, ILogger<GenericReportController> logger)
        {
            _genericReportService = genericReportService ?? throw new ArgumentNullException(nameof(genericReportService));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }
        [HttpPost]
        [Route("GetGenericReportDetails")]
        public IActionResult GetGenericReport([FromBody] GetGenericReportRequest request)
        {
            var logMessage = new StringBuilder("GetGenericReport method called\n");
            try
            {
                string replaceChars = "";
                if (request.ReplaceChars == null)
                    replaceChars = "";
                else
                    replaceChars = request.ReplaceChars;

                var reportDetailItems = _genericReportService.GetReportDetails(request.ReportId, replaceChars);
                return new Responses().OkResult(reportDetailItems);
            }
            catch (Exception e)
            {
                var model = new QueryResult();
                logMessage.AppendLine("Failed calling _genericReportService.GetReportDetails(" + request.ReportId.ToString() + ")");
                logMessage.AppendLine(e.Message);
                logMessage.AppendLine(e.StackTrace);
                _logger.LogError(logMessage.ToString(), null);
                return new Responses().BadRequestResult(model);
            }
        }
    }
}