using System;
using System.Net;
using System.Net.Http;

namespace WeatherAPI.Exceptions
{
    public class RequestException : Exception
    {
        public HttpResponseMessage response;
        public HttpStatusCode statusCode;
        public string message;

        public RequestException(HttpResponseMessage response)
        {
            this.response = response;
            this.statusCode = response.StatusCode;
        }

        public RequestException(HttpResponseMessage response, string message)
        {
            this.response = response;
            this.message = message;
            this.statusCode= response.StatusCode;
        }

        public RequestException(HttpStatusCode statusCode, string message)
        {
            this.statusCode = statusCode;
            this.message = message;
        }
    }
}