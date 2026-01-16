using System.Net;
using System.Net.Http;

namespace BobApi.Entities
{
    public class BobApiError
    {
        private BobApiError(ErrorType type, HttpStatusCode? statusCode = null, string requestInfo = null)
        {
            Type = type;
            StatusCode = statusCode;
            RequestInfo = requestInfo;
        }

        public ErrorType Type { get; }
        public HttpStatusCode? StatusCode { get; }
        public string RequestInfo { get; }

        internal static BobApiError NodeIsUnavailable() => new BobApiError(ErrorType.NodeIsUnavailable, null);
        internal static BobApiError UnsuccessfulResponse(HttpResponseMessage httpResponseMessage)
            => new BobApiError(ErrorType.UnsuccessfulResponse, httpResponseMessage.StatusCode,
                $"{httpResponseMessage.RequestMessage.Method}: {httpResponseMessage.RequestMessage.RequestUri}");

        public override string ToString()
        {
            return Type switch
            {
                ErrorType.UnsuccessfulResponse => $"Request \"{RequestInfo}\", response code: {StatusCode}",
                _ => Type.ToString()
            };
        }
    }

    public enum ErrorType
    {
        NodeIsUnavailable,
        UnsuccessfulResponse
    }
}