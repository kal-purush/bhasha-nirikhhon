using System.Net.Http;
using System.Security.Authentication;
using System.Text.Json;
using System.Threading.Tasks;

namespace Remoteit.RestApi
{
    internal class RemoteitApiRequest<T> : IRemoteitApiRequest<T>
    {
        public async Task<T> SendAsync(HttpClient httpApiClient, HttpRequestMessage httpRequest)
        {
            try
            {
                HttpResponseMessage httpResponse = await httpApiClient.SendAsync(httpRequest);
                var rawResponseBody = await httpResponse.Content.ReadAsStringAsync();

                if (httpResponse.IsSuccessStatusCode)
                {
                    return JsonSerializer.Deserialize<T>(rawResponseBody);
                }

                throw new AuthenticationException(rawResponseBody);
            }
            catch (HttpRequestException apiRequestError)
            {
                throw new AuthenticationException(apiRequestError.Message);
            }
        }
    }
}