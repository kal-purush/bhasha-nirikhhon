using MyCalendar.Mobile.Common.Services.RestService;
using System;
using System.Threading.Tasks;
using Xamarin.Essentials;

namespace MyCalendar.Mobile.Common.Services.Authentication
{
    class AuthenticationService : IAuthenticationService
    {
        private readonly AppSettingsManager appSettingsManager;
        private readonly IRestService restService;

        public AuthenticationService(
            AppSettingsManager appSettingsManager,
            IRestService restService)
        {
            this.appSettingsManager = appSettingsManager ?? throw new ArgumentNullException(nameof(appSettingsManager));
            this.restService = restService ?? throw new ArgumentNullException(nameof(restService));
        }

        public async Task<string> LoginAsync()
        {
            var authResult = await WebAuthenticator.AuthenticateAsync(
                new Uri(this.appSettingsManager["Authorization:Address"] + this.appSettingsManager["Authorization:Endpoint"]),
                new Uri(this.appSettingsManager["Authorization:Callback"] + "://"));

            return authResult?.AccessToken;
        }

        public async Task<bool> LogoutAsync()
        {
            var endpoint = this.appSettingsManager["Authorization:Address"] + this.appSettingsManager["Authorization:Endpoint"];
            return (bool)await this.restService.PostAsync(endpoint);
        }
    }
}