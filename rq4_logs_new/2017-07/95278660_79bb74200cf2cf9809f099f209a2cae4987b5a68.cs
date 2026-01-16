using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using DataParser.HelperClasses;
using System.Text.RegularExpressions;

namespace DataParser.ParserExamples
{
    static class GeoContExample
    {

        public static void Parse()
        {
            
            var URL = @"http://www.geokont.ru";
            var badString = "НЕТ В НАЛИЧИИ";

            var singlePropertiesProduct = new Dictionary<string, Search<string>>
            {
                ["Наименование"] = (node, args) => node
                    .SelectSingleNode(@"//*[@class='prodHead']/h3")
                    .InnerText,
                ["Цена"] = (node, args) => {
                    var a = Regex.Replace(node
.SelectSingleNode(@"//*[@class='prodCard'][2]/p[4]/text()")
.InnerText, @"\s+", string.Empty);
                    return a.Substring(0, a.Length - 7).Replace(" ", "");
                },
                ["Описание"] = (node, args) => node
                    .SelectSingleNode(@"//*[@class='prodCard'][2]/p[1]/text()")
                    ?.InnerText.Replace(badString, string.Empty) ?? string.Empty,
                ["Валюта"] = (node, o) => "RUB",
                [@"""Доступен для заказа"""] = (node, o) => "1",
                [@"Статус"] = (node, o) => "1",
            };

            singlePropertiesProduct["Заголовок"] =
               (node, args) => singlePropertiesProduct["Наименование"](node, args);
            singlePropertiesProduct[@"""Ссылка на витрину"""] = (node, args) =>
                Humanization.GetHumanLink(singlePropertiesProduct["Наименование"](node, args));

            var parser = new LiquiMolyClass(
                isCategory: node => node
                    ._SelectNodes(@"//*[@class='cardsPr']/div/*[@class='smallProdTbl']")
                    .Count != 0,
                findProducts: (node, args) =>
                {
                    var a = node
._SelectNodes(@"//*[@class='container-fluid body-content']/div[last()]//td[@class='smProdHead']/a")
.Select(x => new ArgumentObject(URL + x.Attributes["href"].Value))
.ToArray();
                    return a;
                },
                singlePropertiesCategory: new Dictionary<string, Search<string>>
                {
                    ["Наименование"] = (node, args) => {
                        var b = node.InnerText;
                        var a = new string('!', (int)args.Args[0]) +
Regex.Match(node.InnerHtml, @"<h2 class=""grpHead"">(.*?)</h2>", RegexOptions.Singleline).Groups[1].ToString().Trim();
                        return a;
                    }
                },
                singlePropertiesProduct: singlePropertiesProduct,
                pluralPropertiesProduct: new Dictionary<string, Search<string[]>>
                {
                    ["Изображения"] = (node, args) => {
                        var a = node
._SelectNodes(@"//a[@id='popupImg']")
.Select(x => URL + x.Attributes["href"].Value)
.ToArray();
                        return a;
                        }
                },
                encoding: Encoding.UTF8
            );

            var argument = new ArgumentObject(
                url: @"http://www.geokont.ru/ProductGroup/",
                args: new object[] { 2 });

            var collection =
            parser.GetProductOrCategory(parser.GetLinks(argument,
                @"//*[@class='vBlock' and position() < last()]/a",
                prefix: URL));

            collection = new[]
            {
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "Temporary"}, isCategory: true),
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "!GeoCont"}, isCategory: true)
            }.Extend(collection);

            Import.Write(path: "../../../CSV/geoCont.csv",
                collection: collection.ToArray(),
                headers: Constants.WebAsystKeys,
                format: Constants.WebAsystFormatter);
        }

    }
}