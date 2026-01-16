using AzureKeyVaultWeb.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Diagnostics;

namespace AzureKeyVaultWeb.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly IConfiguration _config;
        private TwilioSettings _twilioSettings;
        private readonly IOptions<TwilioSettings> _twilioOptions;
        private readonly IOptions<SocialLoginSettings> _socialLoginOptions;

        public HomeController(
            ILogger<HomeController> logger, 
            IConfiguration config,
            IOptions<TwilioSettings> twilioOptions,
            IOptions<SocialLoginSettings> socialLoginOptions,
            TwilioSettings twilioSettings)
        {
            _logger = logger;
            _config = config;
            _twilioOptions = twilioOptions;
            _socialLoginOptions = socialLoginOptions;
            //_twilioSettings = new TwilioSettings();
            //config.GetSection("Secrets:Twilio").Bind(_twilioSettings);
            _twilioSettings = twilioSettings;
        }

        public IActionResult Index()
        {
            var connectionString = _config.GetConnectionString("AppSettingsManagerDB");

            //var sendGridApiKey = _config.GetValue("SendGridKey").Value;
            //var sendGridApiKey = _config.GetValue<string>("SendGridKey");

            //var sendGridApiKey = _config.GetSection("Secrets").GetSection("SendGrid").GetValue<string>("ApiKey");
            //var twilioAuthToken = _config.GetSection("Secrets").GetSection("Twilio").GetValue<string>("AuthToken");
            //var twilioAccSid = _config.GetSection("Secrets").GetSection("Twilio").GetValue<string>("AccountSid");
            var sendGridApiKey = _config.GetValue<string>("Secrets:SendGrid:ApiKey");
            //var twilioAuthToken = _config.GetValue<string>("Secrets:Twilio:AuthToken");
            //var twilioAccSid = _config.GetValue<string>("Secrets:Twilio:AccountSid");
            //var twilioPhoneNum = _twilioSettings.PhoneNumber;

            ViewBag.SendGridApiKey = sendGridApiKey;
            //ViewBag.TwilioAuthToken = twilioAuthToken;
            //ViewBag.TwilioAccSid = twilioAccSid;
            //ViewBag.TwilioPhoneNum = twilioPhoneNum;

            // With IOptions
            //ViewBag.TwilioAuthToken = _twilioOptions.Value.AuthToken;
            //ViewBag.TwilioAccSid = _twilioOptions.Value.AccountSID;
            //ViewBag.TwilioPhoneNum = _twilioOptions.Value.PhoneNumber;

            // With Extension Method
            ViewBag.TwilioAuthToken = _twilioSettings.AuthToken;
            ViewBag.TwilioAccSid = _twilioSettings.AccountSID;
            ViewBag.TwilioPhoneNum = _twilioSettings.PhoneNumber;

            ViewBag.FacebookKeyValue = _socialLoginOptions.Value.FacebookSettings.Key + " - " +  _socialLoginOptions.Value.FacebookSettings.Value;
            ViewBag.GoogleKeyValue = _socialLoginOptions.Value.GoogleSettings.Key + " - " + _socialLoginOptions.Value.GoogleSettings.Value;

            ViewBag.ConnectionString = connectionString.Substring(0, 60) + "...";

            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View();
        }
    }
}