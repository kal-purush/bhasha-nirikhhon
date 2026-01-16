using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Server.ProtectedBrowserStorage;
using System.Net.Http.Headers;
using System.Text.Json;
using TestBlazor3K.ApiRequest.Model;

namespace TestBlazor3K.ApiRequest
{
    public class AuthTokenHandler: DelegatingHandler
    {
        private readonly ProtectedLocalStorage _sessionStorage;
        private readonly NavigationManager _navigationManager;

        public AuthTokenHandler(ProtectedLocalStorage sessionStorage, NavigationManager navigationManager, HttpClient httpContext)
        {
            _sessionStorage = sessionStorage;
            _navigationManager = navigationManager;
            InnerHandler = new HttpClientHandler();
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var tokenResult = await _sessionStorage.GetAsync<string>("JWT");

            if (tokenResult.Success && !string.IsNullOrEmpty(tokenResult.Value))
            {
                var a = new AuthenticationHeaderValue("Bearer", tokenResult.Value);
                request.Headers.Authorization = a;
            }

            var response = await base.SendAsync(request, cancellationToken);

            if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
            {
                _navigationManager.NavigateTo("/login");
            }

            return response;
        }
    }
}