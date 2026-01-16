using CsvHelper;
using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace mockdraft_2020
{
    class Program
    {
        static void Main(string[] args)
        {
            var webGet = new HtmlWeb();
            webGet.UserAgent = "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:31.0) Gecko/20100101 Firefox/31.0";
            var document1 = webGet.Load("https://www.drafttek.com/2020-NFL-Mock-Draft/2020-NFL-Mock-Draft-Round-1.asp");
            var document2 = webGet.Load("https://www.drafttek.com/2020-NFL-Mock-Draft/2020-NFL-Mock-Draft-Round-1b.asp");
            var document3 = webGet.Load("https://www.drafttek.com/2020-NFL-Mock-Draft/2020-NFL-Mock-Draft-Round-2.asp");
            var document4 = webGet.Load("https://www.drafttek.com/2020-NFL-Mock-Draft/2020-NFL-Mock-Draft-Round-3.asp");
            var document5 = webGet.Load("https://www.drafttek.com/2020-NFL-Mock-Draft/2020-NFL-Mock-Draft-Round-4.asp");
            var document6 = webGet.Load("https://www.drafttek.com/2020-NFL-Mock-Draft/2020-NFL-Mock-Draft-Round-6.asp");

            Console.WriteLine("Parsing data...");

            // Document data is of type HtmlAgilityPack.HtmlDocument - need to parse it to find info.
            // I'm pretty sure I'm looking for tables with this attribute: background-image: linear-gradient(to bottom right, #0b3661, #5783ad);

            Console.WriteLine("Hello World!");
        }
    }
}