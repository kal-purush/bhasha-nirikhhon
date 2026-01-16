using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net;
using System.Net.Http.Headers;
using System.Net.NetworkInformation;
using System.Text;
using System.Text.Json;
using System.Web;
using TwitterAPI.Application.Options;
using TwitterAPI.TwiterCore.EventArguments;

namespace TwitterAPI.TwiterCore.Client
{
    public class SampleStreamClient : ISampleStreamClient
    {
        private string? _BearerToken { get; set; }

        private const string _streamEndpoint = "https://api.twitter.com/2/tweets/sample/stream";
        private const string _oauth2TokenEndPoint = "https://api.twitter.com/oauth2/token";

        private readonly ILogger<SampleStreamClient> _logger;

        private readonly HttpClient _httpClient;

        private readonly TwitterAPISettings _twitterAPISettings;

        public event EventHandler? DataReceivedEvent;

        protected void OnClientStreamDataReceivedEvent(ClientTweetReceivedEventArgs dataReceivedEventArgs)
        {
            if (DataReceivedEvent == null)
                return;
            DataReceivedEvent(this, dataReceivedEventArgs);
        }

        public SampleStreamClient(
            HttpClient httpClient,
            ILogger<SampleStreamClient> logger,
            IOptions<TwitterAPISettings> twitterAPISettings
            )
        {
            _httpClient = httpClient;
            _logger = logger;
            _twitterAPISettings = twitterAPISettings.Value;
        }

        /// <summary>
        /// It gets the BearerToken for application only
        /// https://dev.twitter.com/oauth/application-only
        /// </summary>
        private async Task GetBearerTokenAsync()
        {
            //Step 1
            //Concatenate key:secret
            string strBearerRequest = HttpUtility.UrlEncode(
                _twitterAPISettings.ApiKey) + ":" + HttpUtility.UrlEncode(_twitterAPISettings.ApiSecretKey);

            //Convert to base 64
            strBearerRequest = System.Convert.ToBase64String(Encoding.UTF8.GetBytes(strBearerRequest));

            //Step 2
            //Send request to oauth end point and get the bearer token
            _httpClient.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Basic", strBearerRequest);

            var response = await _httpClient.PostAsync(
                _oauth2TokenEndPoint,
                new StringContent("grant_type=client_credentials",
                Encoding.UTF8, "application/x-www-form-urlencoded"));

            var respondyContent = await response.Content.ReadAsStringAsync();

            _BearerToken = JsonSerializer.Deserialize<BearerTokenResponse>(respondyContent)?.access_token ?? string.Empty;
        }

        public async Task StartStream(string requestURIExpansion, int maxTweets, int maxConnectionAttempts)
        {
            //Timeout.Infinite not ideal for the GetBearerTokenAsync client
            //only needed to keep stream alive
            //TODO: create separate service for GetBearerTokenAsync
            _httpClient.Timeout = TimeSpan.FromMilliseconds(Timeout.Infinite);

            if (string.IsNullOrWhiteSpace(_BearerToken))
            {
                await GetBearerTokenAsync();

                if (string.IsNullOrWhiteSpace(_BearerToken))
                    throw new InvalidProgramException($"_BearerToken cannot be empty");
            }

            var requestUri = _streamEndpoint;

            if (!string.IsNullOrWhiteSpace(requestURIExpansion))
                requestUri = $"{_streamEndpoint}?{requestURIExpansion}";

            int maxAttemps = maxConnectionAttempts;
            int attemps = 0;
            int requestCount = 0;

            while (attemps < maxAttemps)
            {
                attemps++;
                try
                {
                    Console.WriteLine("Starting Sample Stream at:" + DateTimeOffset.Now);

                    int tweetsFetch = 0;

                    _httpClient.DefaultRequestHeaders.Authorization =
                        new AuthenticationHeaderValue("Bearer", _BearerToken);

                    var stream = _httpClient.GetStreamAsync(requestUri).Result;

                    try
                    {
                        requestCount++;

                        //Stream opened
                        using (var reader = new StreamReader(stream))
                        {
                            while (!reader.EndOfStream)
                            {

                                //Loop through each twwet
                                do
                                {
                                    if (tweetsFetch == maxTweets)
                                    {
                                        break;
                                    }

                                    string jsonData = await reader.ReadLineAsync();

                                    if (!string.IsNullOrEmpty(jsonData))
                                    {
                                        //Raise an event for a potential client to notify data has been recieved
                                        OnClientStreamDataReceivedEvent(new ClientTweetReceivedEventArgs { StreamDataResponse = jsonData });
                                        tweetsFetch = tweetsFetch + 1;
                                        //Console.WriteLine("tweets fetched:" + tweetsFetch + " at " + DateTimeOffset.Now);
                                    }
                                }
                                while (
                                    NetworkInterface.GetIsNetworkAvailable()
                                    && !reader.EndOfStream
                                    && tweetsFetch <= maxTweets
                                    );
                            }
                        }

                        Console.WriteLine("Ending Sample Stream at:" + DateTimeOffset.Now);

                    }
                    catch (WebException ex)
                    {
                        Console.WriteLine(ex.Message);

                    }
                    catch (Exception ex)
                    {
                        //Something more serious happened.
                        //e.g. not network access
                        //not server exception
                        //probably was never reached
                        Console.WriteLine(ex.Message);
                    }
                    //Double check tries h
                    //if we aren't "trying" again
                    //don't need to wait
                    if (attemps < maxAttemps)
                        System.Threading.Thread.Sleep(System.TimeSpan.FromSeconds(10));
                }
                catch (Exception ex)
                {
                    if (attemps < maxAttemps)
                        System.Threading.Thread.Sleep(System.TimeSpan.FromSeconds(10));
                    Console.WriteLine(ex.Message);
                }
            }
        }
    }
}