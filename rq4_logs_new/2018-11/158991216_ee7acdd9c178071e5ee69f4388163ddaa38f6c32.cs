using HouseholdPlanner.Contracts.Notification;
using HouseholdPlanner.Models.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace HouseholdPlanner.Services.Notification
{
    public class SendGridEmailService : IEmailService
    {
        private readonly HttpClient _httpClient;
        private readonly SendGridOptions _sendGridOptions;
        private readonly EmailOptions _emailOptions;

        public SendGridEmailService(HttpClient httpClient, SendGridOptions sendGridOptions, EmailOptions emailOptions)
        {
            _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
            _sendGridOptions = sendGridOptions ?? throw new ArgumentNullException(nameof(sendGridOptions));
            _emailOptions = emailOptions ?? throw new ArgumentNullException(nameof(emailOptions));
        }

        public async Task SendRegistrationEmail(string to)
        {
            throw new NotImplementedException();
        }

        public async Task SendWelcomeEmail(string to, string name)
        {
            throw new NotImplementedException();
        }

        private void SetupHttpClient()
        {
            _httpClient.DefaultRequestHeaders.Clear();

            _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {_sendGridOptions.SendGridApiKey}");
            _httpClient.DefaultRequestHeaders.Add("Content", "application/json");
        }

        private async Task SendEmail()
        {
            var response = await _httpClient.PostAsync(_sendGridOptions.ApiUrl, new StringContent(""));
        }
    }
}