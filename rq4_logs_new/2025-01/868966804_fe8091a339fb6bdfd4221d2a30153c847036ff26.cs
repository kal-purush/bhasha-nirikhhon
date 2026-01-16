using digioz.Forum.Models;
using digioz.Forum.Services.Interfaces;

namespace digioz.Forum.Helpers
{
    public class ForumSessionHelper
    {
        private readonly IForumSessionService _forumSessionService;

        public ForumSessionHelper(IForumSessionService forumSessionService)
        {
            _forumSessionService = forumSessionService;
        }

        public void AddSession(HttpContext httpContext, string sessionId, string pageName)
        {
            var ipAddressHelper = new IpAddressHelper();
            var doesSessionExist = _forumSessionService.Get(sessionId);
            var dateTimeInt = DateTime.Now.Ticks;
            var ipAddress = ipAddressHelper.GetIpAddress(httpContext);
            var browser = ipAddressHelper.GetBrowser(httpContext);

            if (doesSessionExist == null)
            {
                var session = new ForumSession
                {
                    SessionId = sessionId,
                    SessionUserId = 0,
                    SessionLastVisit = dateTimeInt,
                    SessionStart = dateTimeInt,
                    SessionTime = dateTimeInt,
                    SessionIp = ipAddress,
                    SessionBrowser = browser,
                    SessionForwardedFor = "",
                    SessionPage = pageName,
                    SessionViewOnline = 0,
                    SessionAutoLogin = 0,
                    SessionAdmin = 0,
                    SessionForumId = 0
                };

                _forumSessionService.Add(session);
            }
        }
    }
}