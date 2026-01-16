using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;

namespace MemorySessionProvider
{
    /// <summary>
    /// Get primary key of session item.
    /// </summary>
    public class SessionKeyHelper
    {
        private const string keyprefix = "__session_key__";
        private readonly string applicationid = null;

        public SessionKeyHelper()
        {
            applicationid = HttpRuntime.AppDomainAppId;
        }

        /// <summary>
        /// Get primary key by session id.
        /// </summary>
        /// <param name="sessionid"></param>
        /// <returns></returns>
        public string GetRrimaryKey(string sessionid)
        {
            if (string.IsNullOrEmpty(sessionid))
            {
                return null;
            }

            if (sessionid.StartsWith(keyprefix))
            {
                return sessionid;
            }

            return keyprefix + applicationid + "_" + sessionid;
        }
    }
}