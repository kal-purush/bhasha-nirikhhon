using System.Text;
using HTTPServer.core;

namespace HTTPServer.app
{
    public class BadRequestFilter : IHttpHandler
    {
        private IPathContents _pathContents;
        private IHttpHandler _nextCommand;

        public BadRequestFilter(IPathContents pathContents, IHttpHandler nextCommand)
        {
            _pathContents = pathContents;
            _nextCommand = nextCommand;
        }

        public Reply Execute(Request request)
        {
            if (IsMalformed(request))
            {
                var reply = new Reply();
                reply.StartingLine = Encoding.UTF8.GetBytes("HTTP/1.1 400 Bad Request\r\n");
                return reply;
            }
            return _nextCommand.Execute(request);
        }

        private bool IsMalformed(Request request)
        {
            return !IsValidMethod(request) || !request.HttpVersion.Substring(0, 5).Equals("HTTP/");
        }

        private static bool IsValidMethod(Request request)
        {
            return request.Method.Equals("GET") || request.Method.Equals("POST") || request.Method.Equals("PUT");
        }
    }
}