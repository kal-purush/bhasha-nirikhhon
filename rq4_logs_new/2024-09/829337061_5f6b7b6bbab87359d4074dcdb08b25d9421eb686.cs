using IdentityServiceAPI.Models;
using Newtonsoft.Json;
using System.Net;

namespace IdentityService.FunctionalTests.Setup
{
    public static class IdentityApiHttpClientExtensions
    {
        public static ApplicationUserExternalModel DownloadUser(this HttpClient client, string userName)
        {
            HttpRequestMessage request = new()
            {
                RequestUri = new Uri(TestScenarioBase.IdentityServiceUri + $"GetUserByUserName?userName={userName}"),
                Method = HttpMethod.Get
            };

            var response = client.SendAsync(request).Result;

            if (response.StatusCode != HttpStatusCode.OK)
                return new ApplicationUserExternalModel();

            var responseContent = response.Content.ReadAsStringAsync().Result;
            return JsonConvert.DeserializeObject<ApplicationUserExternalModel>(responseContent);
        }
    }
}