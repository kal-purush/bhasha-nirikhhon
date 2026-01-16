using System.Text;
using HTTPServer.core;

namespace HTTPServer.app
{
    public class BadRequestErrorMessage : IHttpHandler
    {
        private IPathContents _pathContents;

        public BadRequestErrorMessage(IPathContents pathContents)
        {
            _pathContents = pathContents;
        }

        public Reply Execute(Request request)
        {
            var reply = new Reply();
            reply.StartingLine = Encoding.UTF8.GetBytes("HTTP/1.1 400 Bad Request\r\n");
            return reply;
        }
    }
}