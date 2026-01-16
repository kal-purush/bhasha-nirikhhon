using System.Text;
using HTTPServer.core;

namespace HTTPServer.app
{
    public class PutContents : IHttpHandler
    {
        private IPathContents _pathContents;

        public PutContents(IPathContents pathContents)
        {
            _pathContents = pathContents;
        }

        public Reply Execute(Request request)
        {
            return Put(request);
        }

        private Reply Put(Request request)
        {
            var reply = new Reply();
            try
            {
                _pathContents.PutContents(request);
                reply.StartingLine = Encoding.UTF8.GetBytes("HTTP/1.1 200 OK\r\n");
            }
            catch
            {
                reply.StartingLine = Encoding.UTF8.GetBytes("HTTP/1.1 400 Bad Request\r\n");
            }
            return reply;
        }
    }
}