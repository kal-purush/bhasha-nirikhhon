using System;

namespace SimpleSmtpInterceptor.Lib.Exceptions
{
    public class InvalidEmailException
        : Exception
    {
        public enum Part
        {
            From,
            To,
            Subject
        }

        public InvalidEmailException(Part part)
            : base(GetMessage(part))
        {

        }

        private static string GetMessage(Part part)
        {
            return $"This email is invalid because the {part.ToString()} is null. 0x202002112207";
        }
    }
}