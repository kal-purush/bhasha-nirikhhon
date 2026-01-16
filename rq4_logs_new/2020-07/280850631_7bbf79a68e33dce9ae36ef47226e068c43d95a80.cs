using Covid19TemperatureAPI.Entities.Data;
using Covid19TemperatureAPI.Helper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace Covid19TemperatureAPI.Controllers
{
    [Route("c19server")]
    [ApiController]
    [Authorize(AuthenticationSchemes = "Bearer")]
    public class ContactTracingController : ControllerBase
    {
        private readonly IConfiguration Configuration;
        private readonly ILogger<ContactTracingController> _logger;
        private ApplicationDbContext Context { get; set; }

        private enum ResponseCodes
        {
            [Display(Name = "Successful")]
            Successful = 1200,
            [Display(Name = "Error")]
            SystemError = 1201,
        }

        public ContactTracingController(IConfiguration configuration, ApplicationDbContext applicationDbContext, ILogger<ContactTracingController> logger)
        {
            Configuration = configuration;
            Context = applicationDbContext;
            _logger = logger;
        }

        /// <summary>
        /// GetQueriedPersonDetails API. Returns the person details by an alert timestamp.
        /// </summary>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST /c19server/getqueriedpersondetails
        ///     {
        ///         "alertTimestamp":"2020-07-28 11:57:56"
        ///     }
        ///      
        /// Sample response:
        /// 
        ///     {
        ///         "respcode": 1200,
        ///         "description": "Successful",
        ///         "queriedPerson": {
        ///             "personName": "Abhishek",
        ///             "personImage": "data:image/jpeg;base64,/9j/4A...uf/9k=",
        ///             "personAlertImage": "data:image/jpeg;base64,/9j/4AAQSkZ...j//Z",
        ///             "isVisitor": false
        ///         }
        ///     }
        /// Response codes:
        ///     1200 = "Successful"
        ///     1201 = "Error"
        /// </remarks>
        /// <returns>
        /// </returns>

        [HttpPost]
        [Route("getqueriedpersondetails")]
        public ActionResult GetQueriedPersonDetails([FromBody]JObject alertTimestamp)
        {
            try
            {
                _logger.LogInformation("GetQueriedPersonDetails() called from: " + HttpContext.Connection.RemoteIpAddress.ToString());

                var received = new { AlertTimestamp = string.Empty };

                received = JsonConvert.DeserializeAnonymousType(alertTimestamp.ToString(Formatting.None), received);

                _logger.LogInformation($"Paramerters: {received.AlertTimestamp}");

                DateTime.TryParse(received.AlertTimestamp, out DateTime dtAlertTimestamp);

                // Check if it's a temperature alert
                var temperatureRecord = Context.TemperatureRecords
                    //.Where(x => x.Timestamp.ToString().Remove(x.Timestamp.ToString().Length - 8) == received.AlertTimestamp)
                    .Where(x => x.Timestamp.AddMilliseconds(-x.Timestamp.Millisecond) == dtAlertTimestamp)
                    ?.Select(x => new { x.PersonName, x.PersonUID, x.ImageBase64 })
                    ?.FirstOrDefault();

                // It is a temperature alert
                if (temperatureRecord != null)
                {
                    // Find employee details
                    var PersonImage = Context.Employees
                        .Where(x => x.UID == temperatureRecord.PersonUID)
                        ?.Select(x => x.ImageBase64)
                        ?.FirstOrDefault();

                    var queriedPerson = new
                    {
                        temperatureRecord.PersonName,
                        PersonImage = PersonImage ?? temperatureRecord.ImageBase64,
                        PersonAlertImage = temperatureRecord.ImageBase64,
                        IsVisitor = PersonImage == null ? true : false
                    };

                    return new JsonResult(new
                    {
                        respcode = ResponseCodes.Successful,
                        description = ResponseCodes.Successful.DisplayName(),
                        queriedPerson
                    });
                }
                else
                {
                    // Check if it's a mask alert
                    var maskRecord = Context.MaskRecords
                        .Where(x => x.Timestamp.AddMilliseconds(-x.Timestamp.Millisecond) == dtAlertTimestamp)
                        ?.Select(x => new { x.PersonName, x.PersonUID, x.ImageBase64 })
                        ?.FirstOrDefault();

                    // Find employee details
                    var PersonImage = Context.Employees
                        .Where(x => x.UID == maskRecord.PersonUID)
                        ?.Select(x => x.ImageBase64)
                        ?.FirstOrDefault();

                    var queriedPerson = new
                    {
                        maskRecord.PersonName,
                        PersonImage = PersonImage ?? maskRecord.ImageBase64,
                        PersonAlertImage = maskRecord.ImageBase64,
                        IsVisitor = PersonImage == null ? true : false
                    };

                    return new JsonResult(new
                    {
                        respcode = ResponseCodes.Successful,
                        description = ResponseCodes.Successful.DisplayName(),
                        queriedPerson
                    });
                }
            }
            catch (Exception e)
            {
                _logger.LogError($"Generic exception handler invoked. {e.Message}: {e.StackTrace}");

                return new JsonResult(new
                {
                    respcode = ResponseCodes.SystemError,
                    description = ResponseCodes.SystemError.DisplayName(),
                    Error = e.Message
                });
            }
        }

    }
}