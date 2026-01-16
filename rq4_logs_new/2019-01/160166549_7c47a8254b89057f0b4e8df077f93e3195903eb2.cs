using System;
using System.Runtime.Serialization;

namespace Common.Exceptions
{
    public class LoginFailedException : Exception
    {
        private const string IncorectLoginExceptionMessage = "Invalid credentials entered";

        public LoginFailedException() : base(IncorectLoginExceptionMessage)
        {
        }

        public LoginFailedException(string message) : base(message)
        {
        }

        protected LoginFailedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public LoginFailedException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}