using RestSharp;
using Retrofit.Net.Attributes.Methods;

namespace exercise7
{
    public interface IGetJsonService
    {
        [Get("")]
        RestResponse<bool> GetBusServices();
    }
}