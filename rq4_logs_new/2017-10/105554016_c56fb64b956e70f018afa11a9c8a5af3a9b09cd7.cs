using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace Common.Helpers
{
    public class HttpFileUploadHelper
    {
        public string FileName { get; set; }
        public string ContentType { get; set; }
        public int ContentLength { get; set; }
        public Stream InputStream { get; set; }
        public string FileKey { get; set; }

        public HttpFileUploadHelper()
        {

        }

        public HttpFileUploadHelper(HttpPostedFile postedFile, string fileKey)
        {
            this.FileName = postedFile.FileName;
            this.ContentType = postedFile.ContentType;
            this.ContentLength = postedFile.ContentLength;
            this.InputStream = postedFile.InputStream;
            this.FileKey = fileKey;
        }
    }
}