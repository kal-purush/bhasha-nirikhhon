using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace TCObjectStorageClient.ObjectStorage
{
    public class TCObjectStorage : IObjectStorage
    {
        public async Task<string> PostToken(string tenentName, string userName, string password)
        {
            var url = "https://api-compute.cloud.toast.com/identity/v2.0/tokens";
            HttpClient client = new HttpClient();

            var requestContent = new StringContent(
                $"{{\"auth\": {{\"tenantName\": \"{tenentName}\",\"passwordCredentials\": {{\"username\": \"{userName}\",\"password\": \"{password}\"}}}}}}");
            requestContent.Headers.ContentType = MediaTypeHeaderValue.Parse("application/json");

            var response = await client.PostAsync(url,requestContent);
            var content = await response.Content.ReadAsStringAsync();
            var responseJson = JsonConvert.DeserializeObject<Dictionary<string, JObject>>(content);
            return responseJson["access"]["token"]["id"].ToString();
        }

        public async Task<bool> UploadFile(string token, string account, string containerName, string objectName, byte[] body)
        {
            HttpClient client = new HttpClient();
            var url = $"https://api-storage.cloud.toast.com/v1/{account}/{containerName}/{objectName}";
            client.DefaultRequestHeaders.Add("X-Auth-Token", token);
            var response = await client.PutAsync(url, new ByteArrayContent(body));
            return response.StatusCode == HttpStatusCode.Created;
        }
    }
}