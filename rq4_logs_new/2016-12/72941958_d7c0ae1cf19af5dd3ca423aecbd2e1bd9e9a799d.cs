using Red_Folder.WebCrawl.Helpers;
using Red_Folder.WebCrawl.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Red_Folder.WebCrawl.Command
{
    public class LegacyProcessor : HttpClientBaseProcessor
    {
        public LegacyProcessor(IClientWrapper httpClient) : base(httpClient)
        {
        }

        public override IUrlInfo Process(string url)
        {
            if (CanBeHandled(url))
            {
                return Handle(url);
            }
            else
            {
                return base.Process(url);
            }
        }

        private bool CanBeHandled(string url)
        {
            // Want to remove old blog links
            if (url.ToLower().Contains("blogspot")) return true;
            if (url.ToLower().Contains("blog.red-folder.com")) return true;

            return false;
        }

        private IUrlInfo Handle(string url)
        {
            return new LegacyUrlInfo(url, String.Format("Legacy reference"));
        }
    }
}