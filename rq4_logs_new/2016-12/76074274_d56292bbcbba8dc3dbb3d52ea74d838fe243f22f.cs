using System;
using System.Net.Http;

namespace PrototypeAdal5
{
    public class ApiRequest
    {
        readonly static HttpClient Client = new HttpClient(new HttpClientHandler());

        public static async System.Threading.Tasks.Task<string> GetJson(Uri uri)
        {

            HttpRequestMessage requestMessage = new HttpRequestMessage();

            requestMessage.RequestUri = uri;

            requestMessage.Headers.Accept.Clear();


            requestMessage.Headers.Add("user-agent", "Jenny's Awesome Release Automator App");


            requestMessage.Method = HttpMethod.Get;
            HttpResponseMessage responseMessage = await Client.SendAsync(requestMessage);
            return await responseMessage.Content.ReadAsStringAsync();
        }
    }
}