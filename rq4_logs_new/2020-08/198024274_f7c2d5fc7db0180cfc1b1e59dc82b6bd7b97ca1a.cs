using Nt.Utils.ServiceInterfaces;
using RestSharp;
using System.Threading.Tasks;
using static Nt.Utils.Helper.HttpUtils;

namespace Nt.Utils.Services
{
    public class HttpService : IHttpService
    {
        private RestClient _restClient;
        public HttpService()
        {
            _restClient = new RestClient(ServerUrl);
        }
        public async Task<TResponse> GetAsync<TRequest, TResponse>(string url, TRequest request)
        {
            var restRequest = new RestRequest(url, Method.GET);
            return await _restClient.GetAsync<TResponse>(restRequest);
        }

        public async Task<TResponse> PostAsync<TRequest, TResponse>(string url, TRequest request)
        {
            var restRequest = new RestRequest(url, Method.POST);
            return await _restClient.PostAsync<TResponse>(restRequest);
        }
    }
}