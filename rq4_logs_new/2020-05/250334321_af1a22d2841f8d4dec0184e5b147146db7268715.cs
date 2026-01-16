using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace SchedulearnBackend.UserFriendlyExceptions
{
    public class UnauthorisedException : UserFriendlyException
    {
        public UnauthorisedException() : this(null)
        {
        }

        public UnauthorisedException(string message) : this(message, null)
        {
        }

        public UnauthorisedException(string message, Exception inner) : base(HttpStatusCode.Unauthorized, message, inner)
        {
        }
    }
}