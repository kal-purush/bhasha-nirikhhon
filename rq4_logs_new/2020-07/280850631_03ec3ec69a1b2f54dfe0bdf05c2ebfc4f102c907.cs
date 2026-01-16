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
    [Route("c19server/getallsites")]
    [ApiController]
    [Authorize(AuthenticationSchemes = "Bearer")]
    public class SiteController : ControllerBase
    {
        private IConfiguration Configuration;
        private readonly ILogger<SiteController> _logger;
        private ApplicationDbContext Context { get; set; }

        private enum ResponseCodes
        {
            [Display(Name = "Successful")]
            Successful = 1200,
            [Display(Name = "Error")]
            SystemError = 1201,
        }

        public SiteController(IConfiguration configuration, ApplicationDbContext applicationDbContext, ILogger<SiteController> logger)
        {
            Configuration = configuration;
            Context = applicationDbContext;
            _logger = logger;
        }

        /// <summary>
        /// GetAllSites API. Returns all sites info.
        /// </summary>
        /// <remarks>
        /// Sample request:
        /// 
        ///     POST /c19server/getallsites
        ///      
        /// Sample response:
        /// 
        ///     {
        ///         "respcode": 1200,
        ///         "description": "Successful",
        ///         "sites": [
        ///             {
        ///                 "siteId": 1,
        ///                 "siteName": "Singapore",
        ///                 "siteDescription": "Chai Chee Office"
        ///             },
        ///             {
        ///                 ...
        ///             }
        ///             ...
        ///         ]
        ///     }
        ///     
        /// Response codes:
        ///     1200 = "Successful"
        ///     1201 = "Error"
        /// </remarks>
        /// <returns>
        /// </returns>
        [HttpPost]
        public async Task<ActionResult> GetAllSites()
        {
            try
            {
                _logger.LogInformation("GetAllSites() called from: " + HttpContext.Connection.RemoteIpAddress.ToString());

                var sites = Context.Sites
                    .Select(x => new { x.SiteId, x.SiteName, x.SiteDescription });

                _logger.LogInformation($"Returning sites: {JsonConvert.SerializeObject(sites)}");
                return new JsonResult(new
                {
                    respcode = ResponseCodes.Successful,
                    description = ResponseCodes.Successful.DisplayName(),
                    sites
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