using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ChronoTwitch
{
    public class AccessToken
    {
        private string _clientId;
        private string _clientSecret;
        private const string _authUrl = "https://id.twitch.tv/oauth2/authorize?";
        private const string _grantTypeCredentials = "client_credentials";
        private const string _grantTypeRefreshToken = "refresh_token";
        private const string _responseType = "token";
        private const string _redirectUri = "http://localhost";

        public AccessToken(string clientId, string clientSecret) 
        {
            _clientId = clientId;
            _clientSecret = clientSecret;
        }

        /// <summary>
        /// Not working as intended. Need a way to automatically log in. 
        /// In the meantime, manually get the access token and refresh token from here:
        /// https://id.twitch.tv/oauth2/token?client_id=<see cref="_clientId"/>&client_secret=<see cref="_clientSecret"/>&code=< CODE >&grant_type=authorization_code&redirect_uri=http://localhost
        /// </summary>
        public async Task<string> GetAccessTokenAsync()
        {
            using var httpClient = new HttpClient();

            var requestBody = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("client_id", _clientId),
                new KeyValuePair<string, string>("client_secret", _clientSecret),
                new KeyValuePair<string, string>("grant_type", _grantTypeCredentials),
                new KeyValuePair<string, string>("response_type", _responseType),
                new KeyValuePair<string, string>("redirect_uri", _redirectUri)
            });

            var response = await httpClient.PostAsync(_authUrl, requestBody);
            if(response.IsSuccessStatusCode)
            {
                var result = await response.Content.ReadAsStringAsync();
                return JObject.Parse(result)["access_token"].ToString();
            }

            return null;
        }

        public async Task<Tuple<string, string>> GetRefreshTokenAsync(string refreshToken)
        {
            using var httpClient = new HttpClient();

            var requestBody = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("client_id", _clientId),
                new KeyValuePair<string, string>("client_secret", _clientSecret),
                new KeyValuePair<string, string>("grant_type", _grantTypeRefreshToken),
                new KeyValuePair<string, string>("refresh_token", refreshToken)
            });

            var response = await httpClient.PostAsync(_authUrl, requestBody);
            if (response.IsSuccessStatusCode)
            {
                var result = await response.Content.ReadAsStringAsync();
                return new Tuple<string, string>(JObject.Parse(result)["access_token"].ToString(), JObject.Parse(result)["refresh_token"].ToString());
            }

            throw new Exception("Failed to get access token");
        }
    }
}