using System;
using System.Collections.Generic;
using System.Linq;

namespace Paynter.WitAi.Sessions
{
    public class WitSessionHelper
    {
        private List<WitSession> _sessions = new List<WitSession>();

        public WitSession FindByUserId(string userId)
        {
            return _sessions.FirstOrDefault(u => u.UserId.Equals(userId));
        }

        public WitSession FindBySessionId(string sessionId)
        {
            return _sessions.FirstOrDefault(u => u.WitSessionId.Equals(sessionId));
        }

        public WitSession FindOrCreateSession(string userId)
        {
            var session = FindByUserId(userId);

            if(session != null)
            {
                return session;
            }

            var sessionId = Guid.NewGuid().ToString("N");
            session = new WitSession(userId, sessionId);
            _sessions.Add(session);
            
            return session;
        }

        public void EndSession(WitSession session)
        {
            _sessions.Remove(session);
        }
    }

}