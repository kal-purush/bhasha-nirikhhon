using System;
using System.Threading.Tasks;

namespace ChronoTwitch
{
    public class Authorization
    {
        private ChronoTwitch _api;
        private string _clientId, _clientSecret, _refreshToken;
        private const string _redirectUri = @"http://localhost:8080/redirect/";
        public Authorization(ChronoTwitch api, string clientId, string clientSecret) 
        { 
            _api = api;
            _clientId = clientId;
            _clientSecret = clientSecret;
        }

        public async Task<string> GetAccessToken()
        {
            var server = new WebServer(_redirectUri);

            var auth = await server.Listen();
            var resp = await _api.Auth.GetAccessTokenFromCodeAsync(auth.GetCode, _clientSecret, _redirectUri);
            _refreshToken = resp.RefreshToken;
            return resp.AccessToken;
        }

        public async Task<string> RefreshToken()
        {
            var refresh = await _api.Auth.RefreshAuthTokenAsync(_refreshToken, _clientSecret);
            return refresh.AccessToken;
        }

        private string GetAuthorizationCodeUrl()
        {
            return "https://id.twitch.tv/oauth2/authorize?" +
                   $"client_id={_clientId}&" +
                   $"redirect_uri={_redirectUri}&" +
                   "response_type=code";
        }
    }
}