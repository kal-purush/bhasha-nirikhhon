using HtmlAgilityPack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AgilityParkTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //var html = @"https://www.yelp.com/biz/the-smoking-ribs-garden-grove?osq=Restaurants";
            var html = @"https://www.yelp.com/biz/mo-ran-gak-restaurant-garden-grove";

            HtmlWeb web = new HtmlWeb();

            var htmlDoc = web.Load(html);






            //var node = htmlDoc.DocumentNode.SelectSingleNode("//body/h1");

            //Console.WriteLine(node.OuterHtml);

            var node = htmlDoc.DocumentNode.Descendants("//body/");

            //var node = htmlDoc.DocumentNode.SelectSingleNode("//head/title");
            Console.WriteLine(node);
            Console.ReadLine();
        }
    }
}