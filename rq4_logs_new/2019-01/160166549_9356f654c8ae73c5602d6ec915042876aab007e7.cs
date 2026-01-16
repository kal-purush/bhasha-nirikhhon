using System;
using System.Runtime.Serialization;

namespace Common.Exceptions
{
    public class IncorectLoginException : Exception
    {
        private const string IncorectLoginExceptionMessage = "Incorrect credentials entered";

        public IncorectLoginException() : base(IncorectLoginExceptionMessage)
        {
        }

        public IncorectLoginException(string message) : base(message)
        {
        }

        protected IncorectLoginException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public IncorectLoginException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}