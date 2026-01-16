using DataAccessLayer.Models;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ServiceLayer.KFC_API_Services
{
    public class SSO_API_Service
    {
        const string API_URL = "https://api.kfc-sso.com";
        const string APP_ID = "";
        public static readonly string APISecret = "";

        //public async Task<HttpResponseMessage> DeleteUserFromSSO(User user)
        //{
        //    var auth = new SignatureService();
        //    var timestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        //    var payloadToSign = auth.PreparePayload(user.Id.ToString(), user.Username, timestamp);
        //    var signature = auth.Sign(payloadToSign);
        //    var requestPayload = new DeleteUserFromSSO_DTO
        //    {
        //        AppId = APP_ID,
        //        Email = user.Username,
        //        SsoUserId = user.Id.ToString(),
        //        Timestamp = timestamp,
        //        Signature = signature
        //    };
        //    var response = await PingDeleteUserFromSSORouteAsync(requestPayload);
        //    if (response.IsSuccessStatusCode)
        //    {
        //        return response;
        //    }
        //    throw new KFCSSOAPIRequestException("Delete User from SSO request error.");
        //}

        //public async Task<HttpResponseMessage> PingDeleteUserFromSSORouteAsync(DeleteUserFromSSO_DTO payload)
        //{
        //    HttpClient client = new HttpClient();
        //    var stringPayload = JsonConvert.SerializeObject(payload);
        //    var jsonPayload = new StringContent(stringPayload, Encoding.UTF8, "application/json");
        //    var request = await client.PostAsync(API_URL + "/api/users/appdeleteuser", jsonPayload);
        //    return request;
        //}
    }
}