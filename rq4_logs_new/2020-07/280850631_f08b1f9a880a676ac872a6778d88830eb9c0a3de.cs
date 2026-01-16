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
    public class NotificationConfigController : ControllerBase
    {
        private readonly IConfiguration Configuration;
        private readonly ILogger<NotificationConfigController> _logger;
        private ApplicationDbContext Context { get; set; }

        private enum ResponseCodes
        {
            [Display(Name = "Successful")]
            Successful = 1200,
            [Display(Name = "Error")]
            SystemError = 1201,
        }

        public NotificationConfigController(IConfiguration configuration, ApplicationDbContext applicationDbContext,
            ILogger<NotificationConfigController> logger)
        {
            Configuration = configuration;
            Context = applicationDbContext;
            _logger = logger;
        }

        /// <summary>
        /// GetAlertConfigurations API. Returns all mobile and emails configured in system for alert notification.
        /// </summary>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST /c19server/getalertconfigurations
        ///     {
        ///         "siteid":"1"
        ///     }
        ///      
        /// Sample response:
        /// 
        ///     {
        ///         "respcode": 1200,
        ///         "description": "Successful",
        ///         "mobiles": [
        ///             {
        ///                 "name": "Bill",
        ///                 "mobileNumber": "658131234"
        ///             },
        ///             {
        ///                 ...
        ///             }
        ///             ...
        ///         ],
        ///         "emailAddresses": [
        ///             {
        ///                 "name": "Abhishek",
        ///                 "emailId": "abhishek.t@orange.com"
        ///             },
        ///             {
        ///                 ...
        ///             }
        ///             ...
        ///         ]
        ///     }
        /// Response codes:
        ///     1200 = "Successful"
        ///     1201 = "Error"
        /// </remarks>
        /// <returns>
        /// </returns>

        [HttpPost]
        [Route("getalertconfigurations")]
        public ActionResult GetAlertConfigurations([FromBody]JObject jsiteId)
        {
            try
            {
                _logger.LogInformation("GetAlertConfigurations() called from: " + HttpContext.Connection.RemoteIpAddress.ToString());

                var received = new { SiteId = string.Empty };

                received = JsonConvert.DeserializeAnonymousType(jsiteId.ToString(Formatting.None), received);

                _logger.LogInformation($"Paramerters: {received.SiteId}");

                if (!int.TryParse(received.SiteId, out int nSiteId)) throw new Exception("Invalid Site Id");

                var mobiles = Context.AlertMobileNumbers
                    .Where(x => x.SiteId == nSiteId)
                    .Select(x => new
                    {
                        x.Name,
                        x.MobileNumber
                    });

                var emailAddresses = Context.AlertEmailAddresses
                    .Where(x => x.SiteId == nSiteId)
                    .Select(x => new
                    {
                        x.Name,
                        x.EmailId
                    });

                return new JsonResult(new
                {
                    respcode = ResponseCodes.Successful,
                    description = ResponseCodes.Successful.DisplayName(),
                    mobiles,
                    emailAddresses
                });

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