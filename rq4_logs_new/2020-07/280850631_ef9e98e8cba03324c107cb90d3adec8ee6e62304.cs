using Covid19TemperatureAPI.Entities.Data;
using Covid19TemperatureAPI.Helper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
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
    public class SettingsController: ControllerBase
    {
        private readonly IConfiguration Configuration;
        private readonly ILogger<SettingsController> _logger;
        private ApplicationDbContext Context { get; set; }

        private enum ResponseCodes
        {
            [Display(Name = "Successful")]
            Successful = 1200,
            [Display(Name = "Error")]
            SystemError = 1201,
        }

        public SettingsController(IConfiguration configuration, ApplicationDbContext applicationDbContext, ILogger<SettingsController> logger)
        {
            Configuration = configuration;
            Context = applicationDbContext;
            _logger = logger;
        }

        /// <summary>
        /// GetTemperatureThreshold API. Returns current temperature threshold value.
        /// </summary>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST /c19server/gettemperaturethreshold
        ///      
        /// Sample response:
        /// 
        ///     {
        ///         "respcode": 1200,
        ///         "description": "Successful",
        ///         "temperatureThreshold": "37.5"
        ///     }
        ///     
        /// Response codes:
        ///     1200 = "Successful"
        ///     1201 = "Error"
        /// </remarks>
        /// <returns>
        /// </returns>
        [HttpPost]
        [Route("gettemperaturethreshold")]
        public ActionResult GetTemperatureThreshold()
        {
            try
            {
                _logger.LogInformation("GetTemperatureThreshold() called from: " + HttpContext.Connection.RemoteIpAddress.ToString());

                var temperatureThreshold = ConfigReader.GetTemperatureThreshold(Context, Configuration).ToString("#.#");

                _logger.LogInformation($"Returning temperatureThreshold: {temperatureThreshold}");
                return new JsonResult(new
                {
                    respcode = ResponseCodes.Successful,
                    description = ResponseCodes.Successful.DisplayName(),
                    temperatureThreshold
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