using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataParser.HelperClasses;

namespace DataParser.ParserExamples
{
    class GratwestExample
    {
        public static void Parse()
        {
            var mainUrl = @"http://www.gratwest.ru";
            var singlePropertiesProduct = new Dictionary<string, Search<string>>
            {
                ["Наименование"] = (node, args) => node
                    .SelectSingleNode(@"//h1")
                    .InnerText.Trim(),
                [@"""Код артикула"""] = (node, args) =>node
                    .SelectSingleNode(@".//div[@class='clearfix' and count(child::div)=3]/div[2]/b[1]")
                    .InnerText,
                ["Цена"] = (node, args) => node
                    .SelectSingleNode(@".//*[contains(@id,'ajax_div_')]/p[2]/nobr/span")
                    .InnerText.Replace("руб", string.Empty).Replace(" ", string.Empty),
                ["Размеры"] = (node, args) => node
                    .SelectSingleNode(@".//div[@class='clearfix' and count(child::div)=3]/div[2]/b[3]")
                    .InnerText,
                ["Валюта"] = (node, o) => "RUB",
                [@"""Доступен для заказа"""] = (node, o) => "1",
                [@"Статус"] = (node, o) => "1",
                ["Описание"] = (node, args) => node
                    .SelectSingleNode(@".//*[@id='detail_dop_info']")
                    .InnerHtml + 
                    node.SelectSingleNode(@".//table[@class='catalog-detail']")
                    .InnerHtml,
                [@"""Краткое описание"""] = (node, args) =>
                {
                    var text = node
                        .SelectSingleNode(@".//*[@id='detail_dop_info']")
                        .InnerText;
                    return text.Substring(0, text.Length < 100 ? text.Length : 100) + "...";
                },
            };
            singlePropertiesProduct["Заголовок"] =
                (node, args) => singlePropertiesProduct["Наименование"](node, args);
            singlePropertiesProduct[@"""Ссылка на витрину"""] = (node, args) =>
                Humanization.GetHumanLink(singlePropertiesProduct["Наименование"](node, args));

            var parser = new LiquiMolyClass(
                isCategory: node => node
                    ._SelectNodes(@"//*[@class='catalog-item-sorting']")
                    .Count != 0,
                findProducts: (node, args) => node
                    ._SelectNodes(@"//*[@class='catalog-item-title']/a")
                    .Select(x => new ArgumentObject(mainUrl + x.Attributes["href"].Value))
                    .ToArray(),
                singlePropertiesProduct: singlePropertiesProduct,
                singlePropertiesCategory: new Dictionary<string, Search<string>>
                {
                    ["Наименование"] = (node, args) => new string('!', (int)args.Args[0]) + node
                        .SelectSingleNode(@".//*[@id='breadcrumb']/span")
                        .InnerText,
                },
                pluralPropertiesProduct: new Dictionary<string, Search<string[]>>
                {
                    ["Изображения"] = (node, args) =>node
                            ._SelectNodes(@"//div[@class='clearfix' and count(child::div)=3]/div[1]//img")
                            .Select(x => mainUrl + x.Attributes["src"].Value)
                            .ToArray(),
                    
                },
                encoding: Encoding.Default,
                xPathPagination: (node, args) => Enumerable.Range(2, 11)
                    .Select(x =>
                        new ArgumentObject($"http://www.gratwest.ru/catalog/36355/?elcount=20&PAGEN_1={x}"))
                    .ToArray()
                );
            var argument = new ArgumentObject(
                url: @"http://www.gratwest.ru/catalog/36355/",
                args: new object[] { 2 });
            var collection = parser.GetProductOrCategory(argument);
            collection = new[]
            {
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "Temporary"}, isCategory: true),
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "!Gratwest"}, isCategory: true)
            }.Extend(collection);
            Import.Write(path: @"..\..\..\CSV\gratwest.csv",
                collection: collection.ToArray(),
                headers: Constants.WebAsystKeys,
                format: Constants.WebAsystFormatter);
        }
    }
}