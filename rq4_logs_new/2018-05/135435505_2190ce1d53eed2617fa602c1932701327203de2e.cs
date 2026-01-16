using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebCrawler.Services
{
    public class WebContentRetrievalService : IWebContentRetrievalService
    {
        private HtmlWeb webContentRetriever;
        public WebContentRetrievalService() {
            webContentRetriever = new HtmlWeb();
        }

        public HtmlDocument GetHtmlContentFromUrl (string webPageUrl) {
            HtmlDocument webPage = webContentRetriever.Load(webPageUrl);
            return webPage;
        }
    }
}