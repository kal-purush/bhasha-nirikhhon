using System.Net;
using System.Net.Http;

namespace RestEase
{
    public static class Testing
    {
        public static ApiException CreateApiException(HttpStatusCode statusCode)
            => new ApiException(HttpMethod.Get, null, statusCode, null, null, null, null);
    }
}