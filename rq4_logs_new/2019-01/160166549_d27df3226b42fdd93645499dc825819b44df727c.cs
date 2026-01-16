using System;
using System.Runtime.Serialization;

namespace Common.Exceptions
{
    public class PasswordMismatchException : Exception
    {
        private const string PasswordMismatchExceptionMessage = "Incorrect password entered";

        public PasswordMismatchException() : base(PasswordMismatchExceptionMessage)
        {
        }

        public PasswordMismatchException(string message) : base(message)
        {
        }

        protected PasswordMismatchException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public PasswordMismatchException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}