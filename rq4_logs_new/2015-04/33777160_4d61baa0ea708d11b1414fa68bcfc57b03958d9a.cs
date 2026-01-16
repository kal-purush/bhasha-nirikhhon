using HeroisDoCancer.Models;
using System;
using System.Linq;
using System.Web;

namespace HeroisDoCancer.WebUtils
{
    public class SessionWrapper
    {
        private readonly HttpSessionStateBase session;

        public SessionWrapper(HttpSessionStateBase session)
        {
            if (session == null)
                throw new ArgumentNullException("session");

            this.session = session;
        }

        public Voluntario Voluntario
        {
            get { return (Voluntario)session["Voluntario"]; }
            set { SetSafe("Voluntario", value); }
        }

        public void Close()
        {
            session.Clear();
            session.Abandon();
        }

        protected virtual void SetSafe(String key, Object value)
        {
            if (value == null)
            {
                if (session.Keys.OfType<String>().Any(k => k == key))
                    session.Remove(key);
            }
            else
                session.Add(key, value);
        }
    }
}