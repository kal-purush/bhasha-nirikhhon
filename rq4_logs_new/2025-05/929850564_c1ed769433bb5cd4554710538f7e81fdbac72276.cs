using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace ClientNexus.Domain.Exceptions.ServerErrorsExceptions
{
    public class InvalidInputException : Exception
    {
        public InvalidInputException() { }

        public InvalidInputException(string? message)
            : base(message) { }

        public InvalidInputException(string? message, Exception? innerException)
            : base(message, innerException) { }
    }
}