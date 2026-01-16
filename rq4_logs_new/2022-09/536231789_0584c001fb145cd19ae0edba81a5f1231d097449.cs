// See https://aka.ms/new-console-template for more information
using AptekaParsing;
using HtmlAgilityPack;
using System.Text.RegularExpressions;

WebScraper webScraper = new WebScraper();
string patternUrls = @"https://mypharmacy\.com\.ua/catalogue.*?(?="")";
string patternCount = @"(?<=\ for\ \(var\ i\ =\ 1;\ i\ <=).*?(?=\ \*\ 28)";
var str = await webScraper.GetHtml("https://mypharmacy.com.ua/catalogue/all/");

var ListUrls = Regex.Matches(str, patternUrls).Cast<Match>()
                .Select(m => m.Value).Distinct()
                .ToList();
string count = "";
int countMain = 1;
foreach (var listUrl in ListUrls)
{
    if (listUrl == "https://mypharmacy.com.ua/catalogue/all/") continue;

    int countSymbol = Extensions.CountByCharacter(listUrl, '/');
    if (countSymbol > 5) continue;
    str = await webScraper.GetHtml(listUrl);
    count = Regex.Match(str, patternCount).Value;

    bool flag = Extensions.IsDigit(count, count.Length-1);
    HtmlDocument htmlDoc = new HtmlDocument();
    htmlDoc.LoadHtml(str);
    HtmlAgilityPack.HtmlNodeCollection nodesProduct = htmlDoc.DocumentNode.SelectNodes("//a[@class='products-list__item-link-wrapper']");
    if(flag)
    {
        for (int i = 1; i < Convert.ToInt32(count);)
        {
            foreach (var node in nodesProduct)
            {
                var href = node.Attributes["href"].Value.Replace("instruction/", "");
                str = await webScraper.GetHtml(listUrl);
            }
            i++;
            str = await webScraper.GetHtml(listUrl + $"page={i}/");
            htmlDoc.LoadHtml(str);
            nodesProduct = htmlDoc.DocumentNode.SelectNodes("//a[@class='products-list__item-link-wrapper']");
        }
    }
}