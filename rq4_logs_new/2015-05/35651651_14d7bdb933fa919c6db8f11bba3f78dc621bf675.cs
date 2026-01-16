using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;

namespace Hatfield.EnviroData.FileSystems.HttpFileSystem
{
    public class HttpFileSystem : IFileSystem
    {
        private Uri _uri;

        public HttpFileSystem(string url)
        {
            _uri = new Uri(url);
        }

        public DataFromFileSystem FetchData()
        {
            HttpWebRequest httpWebRequest = (HttpWebRequest)HttpWebRequest.Create(_uri);
            HttpWebResponse httpWebReponse = (HttpWebResponse)httpWebRequest.GetResponse();
            var fileStream = httpWebReponse.GetResponseStream();
            var fileName = Path.GetFileName(_uri.LocalPath);

            return new DataFromFileSystem(fileName, fileStream);
        }


        public IEnumerable<DataFromFileSystem> FetchData(IEnumerable<IFileSystemFilter> filters)
        {
            throw new NotImplementedException();
        }
    }
}