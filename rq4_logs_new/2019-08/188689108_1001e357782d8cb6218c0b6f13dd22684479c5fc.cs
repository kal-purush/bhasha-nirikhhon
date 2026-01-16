using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using vopen_api.Models.legacy;
using vopen_api.Repositories;
using static vopen_api.Models.legacy.LegacyEventbriteAttendeesResponse;

namespace vopen_api.Controllers
{
    [Route("legacy/api/v1/{edition}")]
    [ApiController]
    public class LegaciesController : ControllerBase
    {
        private readonly EditionsRepository editionsRepository;
        private readonly IConfiguration configuration;
        private readonly string eventbriteEdition;
        private readonly string MOBILE_APP_USER = "app";
        private readonly string MOBILE_APP_TOKEN = "66197FD1-6C77-4D20-A10D-D27BF2B7D053";

        public LegaciesController(EditionsRepository editionsRepository, IConfiguration configuration)
        {
            this.editionsRepository = editionsRepository;
            this.configuration = configuration;
            this.eventbriteEdition = configuration.GetSection("Edition").Value; // e.g vopen-ar-2019
        }

        //devuelve modelo LegacySpronsor
        [HttpPost("Sponsors")]
        public async Task<IActionResult> Sponsors()
        {
            throw new NotImplementedException();
        }

        //devuelve modelo ConfSponsors
        [HttpPost("ConfSponsors")]
        public async Task<IActionResult> ConfSponsors()
        {
            throw new NotImplementedException();
        }

        //devuelte integer
        [HttpPost("Editions")]
        public async Task<IActionResult> Editions()
        {
            throw new NotImplementedException();
        }

        [HttpPost("AppConfig")]
        public async Task<IActionResult> AppConfig()
        {
            throw new NotImplementedException();
        }

        // Eventbrite endpoint 
        [HttpPost("IsValidAttendee")]
        public async Task<IActionResult> IsValidAttendee(LegacyIsValidAttendeeDTO attendeeInfo)
        {
            if (!this.IsValidRequest(attendeeInfo))
            {
                return Unauthorized();
            }

            Attendee result = null;

            var configurationEventBrite = configuration.GetSection("EventBrite");
            var eventBriteId = configurationEventBrite.GetSection("EventbriteEventId").Value;
            var eventBriteUSer = configurationEventBrite.GetSection("User").Value;
            var eventBriteKey = configurationEventBrite.GetSection("Key").Value;
            var eventBriteClientSecret = configurationEventBrite.GetSection("ClientSecret").Value;
            var eventBriteOAuthTokenPersonal = configurationEventBrite.GetSection("OAuthTokenPersonal").Value;
            var eventBriteOAuthTokenAnonymous = configurationEventBrite.GetSection("OAuthTokenAnonymous").Value;
            var eventBriteAttendeesAddress = configurationEventBrite.GetSection("AttendeesAddress").Value + "attendees/?token=" + eventBriteOAuthTokenPersonal;
            var eventbriteTicketNamesAllowedInRaffle = configurationEventBrite.GetSection("EventbriteTicketNamesAllowedInRaffle").Value;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;

            string attendeesRawResponse = "";
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(eventBriteAttendeesAddress);
            request.Accept = "application/json";

            try
            {
                using (WebResponse response = await request.GetResponseAsync())
                using (Stream dataStream = response.GetResponseStream())
                using (StreamReader reader = new StreamReader(dataStream))
                {
                    attendeesRawResponse = reader.ReadToEnd();
                }

                var attendeesResponse = JsonConvert.DeserializeObject<LegacyEventbriteAttendeesResponse>(attendeesRawResponse);

                if (attendeesResponse != null && eventbriteTicketNamesAllowedInRaffle.ToLower().Contains(result.ticket_class_name.ToLower()) == false)
                {
                    // Found but ticket not allowed.
                    result = null;
                }
                else
                {
                    result = attendeesResponse.attendees.FirstOrDefault(a => a.profile.email.ToLowerInvariant() == attendeeInfo.Email.ToLowerInvariant());
                }

                return Ok(result);
            }
            catch (Exception exception)
            {
                return BadRequest(exception);
            }
        }

        private bool IsValidRequest(LegacyApiCredentialsDTO attendeeInfo)
        {
            if (attendeeInfo == null)
            {
                return false;
            }

            var isValidUser = attendeeInfo.User != null && attendeeInfo.User.ToLower() == this.MOBILE_APP_USER;
            var isValidToken = attendeeInfo.Token != null && attendeeInfo.Token.ToUpper() == this.MOBILE_APP_TOKEN;
            return isValidUser && isValidToken;
        }
    }
}