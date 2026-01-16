using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PrizeDrawWeb.Services.Meetup
{
    public static class MeetupUrlParser
    {
        public static (string, string) Parse (string url)
            {
             if ( Uri.TryCreate(url,UriKind.Absolute, out var address))
            {
                var group= address.Segments[1].TrimEnd('/');
                var eventId = address.Segments[3].TrimEnd('/');
                return (group,eventId);
            }
             return (null,null);
            }
    }
}