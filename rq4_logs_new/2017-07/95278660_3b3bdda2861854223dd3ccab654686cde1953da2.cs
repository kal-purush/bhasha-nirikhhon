using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using xNet;
using System.Timers;

namespace DataParser
{
    public static class HtmlDownload
    {
        public static string GetHtmlPage(string url)
        {
            var request = new HttpRequest();

            CookieDictionary cookie = new CookieDictionary(false);

            request.Cookies = cookie;
            request.UserAgent = Http.ChromeUserAgent();
            request.KeepAlive = true;
            request.AllowAutoRedirect = false;

            var response = request.Get(url).ToString();
            request.Close();

            return response;
        }
    }
}