using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace SchedulearnBackend.UserFriendlyExceptions
{
    public class BadRequestException : UserFriendlyException
    {
        public BadRequestException() : this(null)
        {
        }

        public BadRequestException(string message) : this(message, null)
        {
        }

        public BadRequestException(string message, Exception inner) : base(HttpStatusCode.BadRequest, message, inner)
        {
        }
    }
}