using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Exceptions
{
    public class BadRequestException : Exception
    {
        public int HttpStatusCode { get; init; }
        public object? Error { get; init; }
        public string ErrorCode { get; init; } = default!;

        public BadRequestException(string message, string errorCode, int statusCode, object? error = null) : base(message)
        {
            HttpStatusCode = statusCode;
            Error = error;
            ErrorCode = errorCode;
        }

        public BadRequestException(string message, Exception innerException) : base(message, innerException)
        { }

    }
}