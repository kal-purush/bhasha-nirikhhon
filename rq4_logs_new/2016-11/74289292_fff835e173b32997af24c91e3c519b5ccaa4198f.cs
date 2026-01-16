using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace BlinkSharp
{
    public class Blink
    {
        private BlinkAuthInfo authInfo;
        private BlinkNetworksInfo networksInfo;
        private BlinkNetworkEvents eventsInfo;

        public Blink()
        {

        }

        public async Task<bool> Connect(string username, string password)
        {
            try
            {
                var payload = new BlinkCredentials
                {
                    email = username,
                    password = password,
                    client_specifier = "iPhone 9.2 | 2.2 | 222"
                };

                // Serialize our concrete class into a JSON String
                var stringPayload = await Task.Run(() => JsonConvert.SerializeObject(payload));

                // Wrap our JSON inside a StringContent which then can be used by the HttpClient class
                var httpContent = new StringContent(stringPayload, Encoding.UTF8, "application/json");

                using (var httpClient = new HttpClient())
                {

                    // Do the actual request and await the response
                    var httpResponse = await httpClient.PostAsync("https://prod.immedia-semi.com/login", httpContent);

                    // If the response contains content we want to read it!
                    if (httpResponse.Content != null)
                    {
                        string responseContent = await httpResponse.Content.ReadAsStringAsync();

                        // From here on you could deserialize the ResponseContent back again to a concrete C# type using Json.Net
                        authInfo = JsonConvert.DeserializeObject<BlinkAuthInfo>(responseContent);

                        bool networkResult = await PopulateNetworks();

                        return true;
                    }
                }
            }
            catch (Exception)
            {

            }

            return false;
        }

        public async Task<byte[]> GetLatestThumbnail(string cameraName)
        {
            bool eventsResult = await PopulateEvents();
            var sortedEvents = eventsInfo._event.Where(k => k.camera_name == cameraName).OrderByDescending(k => k.created_at);
            var latestEvent = sortedEvents.FirstOrDefault();
            if (latestEvent != null)
            {
                string fullVideoUrl = $"https://prod.immedia-semi.com{latestEvent.video_url}";
                string fullThumbnailUrl = Path.ChangeExtension(fullVideoUrl, ".jpg");

                using (var httpClient = new HttpClient())
                {
                    // Do the actual request and await the response
                    httpClient.DefaultRequestHeaders.Add("Host", "prod.immedia-semi.com");
                    httpClient.DefaultRequestHeaders.Add("TOKEN_AUTH", authInfo.authtoken.authtoken);
                    var httpResponse = await httpClient.GetAsync(fullThumbnailUrl);

                    // If the response contains content we want to read it!
                    if (httpResponse.Content != null)
                    {
                        var responseContent = await httpResponse.Content.ReadAsByteArrayAsync();
                        return responseContent;
                    }
                }
            }

            return null;
        }

        private async Task<bool> PopulateNetworks()
        {
            try
            {
                using (var httpClient = new BlinkHttpClient(authInfo.authtoken.authtoken))
                {
                    networksInfo = await httpClient.GetJsonAsync<BlinkNetworksInfo>("networks");
                    return true;
                }
            }
            catch (Exception) { }

            return false;
        }

        private async Task<bool> PopulateEvents()
        {
            try
            {
                using (var httpClient = new BlinkHttpClient(authInfo.authtoken.authtoken))
                {
                    eventsInfo = await httpClient.GetJsonAsync<BlinkNetworkEvents>($"events/network/{networksInfo.networks.First().id}");
                    return true;
                }
            }
            catch (Exception) { }

            return false;
        }
    }
}