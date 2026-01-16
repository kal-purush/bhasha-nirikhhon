using LagDaemon.YAMUD.Model.Utilities;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace LagDaemon.YAMUD.WebClient.Services
{
    public class PluginsService : IPluginsService
    {
        private readonly HttpClient _httpClient;
        private readonly IAuthenticationService _authenticationService;

        public PluginsService(HttpClient httpClient, IAuthenticationService authenticationService)
        {
            _httpClient = httpClient;
            _authenticationService = authenticationService;
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _authenticationService.Authority.AuthToken.token);
        }

        public async Task<IEnumerable<PluginDescription>> GetPlugins()
        {
            var request = new HttpRequestMessage(HttpMethod.Get, "https://localhost:7214/api/Plugin/GetAll");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _authenticationService.Authority.AuthToken.token);
            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadFromJsonAsync<IEnumerable<PluginDescription>>();
        }

        public async Task StopPlugin(Guid pluginId)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, $"https://localhost:7214/api/Plugin/Stop/{pluginId}");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _authenticationService.Authority.AuthToken.token);
            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();
        }

        public async Task StartPlugin(Guid pluginId)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, $"https://localhost:7214/api/Plugin/Start/{pluginId}");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _authenticationService.Authority.AuthToken.token);
            var response = await _httpClient.SendAsync(request);
            response.EnsureSuccessStatusCode();
        }

    }

    public interface IPluginsService
    {
        Task<IEnumerable<PluginDescription>> GetPlugins();
        Task StopPlugin(Guid pluginId);
        Task StartPlugin(Guid pluginId);
    }


}