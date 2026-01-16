using BroadwayBuilder.Api.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace BroadwayBuilder.Api.Controllers
{
    [RoutePrefix("api")]
    public class UserAnalyticsController : ApiController
    {
        
        [Route("NumberOfSuccessfulLogins")]
        [HttpGet]
        public IHttpActionResult NumberOfSuccessfulLogins(int month, int year)
        {

            var successfulLoginsPerMonth = new NumberOfSuccessfulLoginsResponseModel()
            {
                Month = month,
                Year = year,
                AverageSuccssfulLogins = 10,
                MinSuccessfulLogins = 2,
                MaxSuccessfulLogins = 15
            };

            return Ok(successfulLoginsPerMonth);

        }
    }
}