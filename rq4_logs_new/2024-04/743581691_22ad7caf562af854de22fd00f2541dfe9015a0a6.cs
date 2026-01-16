using RestSharp;
using Test.Ultility;
using Xunit.Abstractions;

namespace Test.AuthenticationFeature
{
    public class ForgotPassword
    {
        private readonly ITestOutputHelper _output;

        public ForgotPassword(ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public async Task UTC01_SendOTPSuccess()
        {
            // Rest client
            var restClient = new RestClient(new RestClientOptions
            {
                BaseUrl = new Uri(Url.AuthenticateFeatureUrl)
            });

            // Rest request
            var restRequest = new RestRequest("/api/ForgortPassword/ForgotPassword");
            string payload = $$"""
                {
                  "email": "binhprohp01@gmail.com"
                }
                """;

            restRequest.AddStringBody(payload, DataFormat.Json);

            // Rest response
            var restResponse = await restClient.ExecuteAsync(restRequest, Method.Post);

            if (restResponse.Content != null)
            {
                string response = restResponse.Content;

                // Assert
                Assert.Contains("MSG14", response);
            }

            // Log
            _output.WriteLine(restResponse.Content);
        }

        [Fact]
        public async Task UTC02_InvalidEmail()
        {
            // Rest client
            var restClient = new RestClient(new RestClientOptions
            {
                BaseUrl = new Uri(Url.AuthenticateFeatureUrl)
            });

            // Rest request
            var restRequest = new RestRequest("/api/ForgortPassword/ForgotPassword");
            string payload = $$"""
                {
                  "email": "Binhprohp00@@gmail.com"
                }
                """;

            restRequest.AddStringBody(payload, DataFormat.Json);

            // Rest response
            var restResponse = await restClient.ExecuteAsync(restRequest, Method.Post);

            if (restResponse.Content != null)
            {
                string response = restResponse.Content;

                // Assert
                Assert.Contains("MSG09", response);
            }

            // Log
            _output.WriteLine(restResponse.Content);
        }

        [Fact]
        public async Task UTC03_NotExistEmail()
        {
            // Rest client
            var restClient = new RestClient(new RestClientOptions
            {
                BaseUrl = new Uri(Url.AuthenticateFeatureUrl)
            });

            // Rest request
            var restRequest = new RestRequest("/api/ForgortPassword/ForgotPassword");
            string payload = $$"""
                {
                  "email": "Test@gmail.com"
                }
                """;

            restRequest.AddStringBody(payload, DataFormat.Json);

            // Rest response
            var restResponse = await restClient.ExecuteAsync(restRequest, Method.Post);

            if (restResponse.Content != null)
            {
                string response = restResponse.Content;

                // Assert
                Assert.Contains("MSG10", response);
            }

            // Log
            _output.WriteLine(restResponse.Content);
        }

        [Fact]
        public async Task UTC04_NullEmail()
        {
            // Rest client
            var restClient = new RestClient(new RestClientOptions
            {
                BaseUrl = new Uri(Url.AuthenticateFeatureUrl)
            });

            // Rest request
            var restRequest = new RestRequest("/api/ForgortPassword/ForgotPassword");
            string payload = $$"""
                {
                  "email": ""
                }
                """;

            restRequest.AddStringBody(payload, DataFormat.Json);

            // Rest response
            var restResponse = await restClient.ExecuteAsync(restRequest, Method.Post);

            if (restResponse.Content != null)
            {
                string response = restResponse.Content;

                // Assert
                Assert.Contains("MSG11", response);
            }

            // Log
            _output.WriteLine(restResponse.Content);
        }
    }
}