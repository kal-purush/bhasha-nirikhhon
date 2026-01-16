using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace SchedulearnBackend.UserFriendlyExceptions
{
    public class LimitUsed : UserFriendlyException
    {
        public LimitUsed() : this(null)
        {
        }

        public LimitUsed(string message) : this(message, null)
        {
        }

        public LimitUsed(string message, Exception inner) : base(HttpStatusCode.Forbidden, message, inner)
        {
        }
    }
}