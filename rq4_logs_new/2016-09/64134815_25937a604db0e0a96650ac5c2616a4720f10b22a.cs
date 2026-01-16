using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using HTTPServer.core;
using System.IO;

namespace HTTPServer.app
{
    public class DeleteContents : IHttpHandler
    {
        private IPathContents _pathContents;

        public DeleteContents(IPathContents pathContents)
        {
            _pathContents = pathContents;
        }

        public Reply Execute(Request request)
        {
            if(IsValidFile(request.Uri,_pathContents))
            { 
                DeleteResourece(request);
                Reply reply = new Reply();
                reply.StartingLine = Encoding.UTF8.GetBytes("HTTP/1.1 200 OK\r\n");
                return reply;
            }
            else
            {
                Reply reply = new Reply();
                reply.StartingLine = Encoding.UTF8.GetBytes("HTTP/1.1 404 Not Found\r\n");
                return reply;
            }
        }

        private void DeleteResourece(Request request)
        {
            var resourceName = _pathContents.DirectoryPath + request.Uri;
            File.Delete(resourceName);
        }

        private bool IsValidFile(string uri, IPathContents pathContents)
        {
            try
            {
                var files = pathContents.GetFiles("");
                foreach (string file in files)
                {
                    var expectedFilePath = GetExpectedFilePath(uri, pathContents);
                    if (expectedFilePath.Equals(file))
                        return true;
                }
                return false;
            }
            catch
            {
                return false;
            }
        }

        private string GetExpectedFilePath(string uri, IPathContents pathContents)
        {
            var expectedFilePath = pathContents.DirectoryPath;
            expectedFilePath += "\\";
            expectedFilePath += uri.Substring(1);
            return expectedFilePath;
        }
    }
}