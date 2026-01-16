using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Web;

namespace falcons
{
    public class BingSearch
    {
        public void BingKeywordGetter(string query)
    {
         // Create a Bing container.

            string rootUri = "https://api.datamarket.azure.com/Bing/Search";

            var bingContainer = new Bing.BingSearchContainer(new Uri(rootUri));

            // Replace this value with your account key.
            string market = "en-us";
            string accountKey = "3OL0xieASOtqcyusoHZ447oH2wCJd/Mkz7gM/+ZjQF0";

            // Configure bingContainer to use your credentials.

            bingContainer.Credentials = new NetworkCredential(accountKey, accountKey);
            var webquery = bingContainer.Web(query, null, null,market, null, null, null, null);
            webquery = webquery.AddQueryOption("$top", 10);
            var webresults = webquery.Execute();

    }
    }
}