using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataParser.HelperClasses;
using System.Text.RegularExpressions;
using HtmlAgilityPack;

namespace DataParser.ParserExamples
{
    static class DynaToneExample
    {
        public static void Parse()
        {
            var URL = @"http://dynatone.ru";
            var addedUrl = @"http://dynatone.ru/products.php?group=993876&type=&brand=FLIGHT+PERCUSSION&cat=0&order=status&orderWay=desc&p=";

            var singlePropertiesProduct = new Dictionary<string, Search<string>>
            {
                ["Наименование"] = (node, args) => node
                    .SelectSingleNode(@"//h1/span[@class='bk_name']")
                    .InnerText,
                [@"""Код артикула"""] = (node, args) => node
                    .SelectSingleNode(@"//td[@valign='middle']/b")
                    .InnerText
                    .Trim(),
                ["Цена"] = (node, args) => node
                    .SelectSingleNode(@"//h1[@align='center']/span")
                    .InnerText
                    .Replace(" ", string.Empty),
                ["Описание"] = (node, args) =>
                {
                    var description = "";
                    var desc = node
                      ._SelectNodes(@"//table[@align='left']/../p[1]")
                      ?.ToArray() ?? new HtmlNode[0];
                    foreach(var e in desc)
                    {
                        description += e.InnerHtml;
                    }
                    return description;
                },
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
                    ._SelectNodes(@"//*[@class='shapka'][2]/a")
                    .Count != 0,
                findProducts: (node, args) => node
                    ._SelectNodes(@"//td[@class='tovname']/*[@class='descrcut']/../a[1]")
                    .Select(x => new ArgumentObject(URL + x.Attributes["href"].Value))
                    .ToArray(),
                singlePropertiesCategory: new Dictionary<string, Search<string>>
                {
                    ["Наименование"] = (node, args) => new string('!', (int)args.Args[0]) + node
                        .SelectSingleNode(@"//*[@class='shapkasearch']/b/a[1]")
                        .InnerText
                        .Trim()
                },
                singlePropertiesProduct: singlePropertiesProduct,
                pluralPropertiesProduct: new Dictionary<string, Search<string[]>>
                {
                    ["Изображения"] = (node, args) => node
                        ._SelectNodes(@"//td[@align='center']/img")
                        .Select(x => URL + x.Attributes["src"].Value)
                        .ToArray()
                },
                encoding: Encoding.Default
            );

            var arguments = new[] { 1, 2, 3, 4, 5, 6 }
               .Select(x => new ArgumentObject(url: addedUrl + x,
                                       args: new object[] { 2 }));

            var collection = parser.GetProductOrCategory(arguments);

            collection = new[]
            {
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "Temporary"}, isCategory: true),
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "!DynaTone"}, isCategory: true)
            }.Extend(collection);

            Import.Write(path: "../../../CSV/dynaTone.csv",
               collection: collection.ToArray(),
               headers: Constants.WebAsystKeys,
               format: Constants.WebAsystFormatter);
        }
    }
}