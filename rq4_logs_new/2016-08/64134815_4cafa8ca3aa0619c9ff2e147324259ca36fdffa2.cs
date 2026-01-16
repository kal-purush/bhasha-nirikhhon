using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HTTPServer.core;

namespace HTTPServer.app
{
    public class DirectoryContentsCriteria : ICriteria
    {
        public bool ShouldRun(Request request)
        {
            return IsRoot(request.Uri) && IsGetMethod(request.Method);
        }

        private bool IsGetMethod(string requestMethod)
        {
            return requestMethod.Equals("GET");
        }

        private bool IsRoot(string uri)
        {
            return uri.Equals("/");
        }
    }
}