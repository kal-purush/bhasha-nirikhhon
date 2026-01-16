using AuthBot.Models;
using Microsoft.Bot.Builder.Dialogs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace AuthBot.Helpers
{
    class VisualStudioOnlineHelper
    {
        public static string GetAuthUrlAsync(ResumptionCookie resumptionCookie)
        {
            var encodedCookie = Microsoft.Bot.Builder.Dialogs.UrlToken.Encode(resumptionCookie);
            return String.Format("{0}/oauth2/authorize?client_id={1}&response_type=Assertion&state={2}&scope={3}&redirect_uri={4}", AuthSettings.EndpointUrl, AuthSettings.ClientId, encodedCookie, AuthSettings.Tenant, AuthSettings.RedirectUrl);
        }
        public static async Task<AuthResult> GetTokenByAuthCodeAsync(string authorizationCode)
        {
            using (var client = new HttpClient())
            {

                client.BaseAddress = new Uri("https://app.vssps.visualstudio.com");
                using (var request = new HttpRequestMessage(HttpMethod.Post, String.Empty))
                {
                    var postData = string.Format("/oauth2/token?client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer&client_assertion={0}&grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion={1}&redirect_uri={2}", AuthSettings.ClientSecret, authorizationCode, AuthSettings.RedirectUrl);
                    request.Content = new StringContent(postData, Encoding.UTF8, "application/x-www-form-urlencoded");
                    HttpResponseMessage response = await client.SendAsync(request);
                    var str = await response.Content.ReadAsStringAsync();

                    //AuthResult authResult = AuthResult.FromADALAuthenticationResult(result, tokenCache);
                    AuthResult authResult = null;
                    return authResult;

                }
            }
        }
    }
}