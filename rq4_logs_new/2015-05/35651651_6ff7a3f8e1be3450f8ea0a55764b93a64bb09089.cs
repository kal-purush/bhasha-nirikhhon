using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Hatfield.EnviroData.FileSystems.FTPFileSystem
{
    public class FTPFileSystem : IFileSystem
    {
        private Uri _uri;
        private NetworkCredential _credentials;

        public FTPFileSystem(string url)
        {
            _uri = new Uri(url);
            _credentials = null;
        }

        public FTPFileSystem(string url, string username, string password)
        {
            _uri = new Uri(url);
            _credentials = new NetworkCredential(username, password);
        }

        public DataFromFileSystem FetchData()
        {
            WebClient request = new WebClient();

            if (_credentials != null)
            {
                request.Credentials = _credentials;
            }

            var fileStream = request.OpenRead(_uri);
            var fileName = System.IO.Path.GetFileName(_uri.LocalPath);

            return new DataFromFileSystem(fileName, fileStream);
        }


        public IEnumerable<DataFromFileSystem> FetchData(IEnumerable<IFileSystemFilter> filters)
        {
            throw new NotImplementedException();
        }
    }
}