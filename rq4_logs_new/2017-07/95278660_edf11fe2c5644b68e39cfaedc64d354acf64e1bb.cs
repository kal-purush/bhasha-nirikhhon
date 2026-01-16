using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DataParser.HelperClasses;

namespace DataParser.ParserExamples
{
    class PlaydoradoExample
    {
        public static void Parse()
        {
            var suffix = @"?limit=1000";
            var prefix = @"https://playdorado.ru/";
            var singlePropertiesProduct = new Dictionary<string, Search<string>>
            {
                ["Наименование"] = (node, args) => node
                    .SelectSingleNode(@"//h1")
                    .InnerText.Trim(),
                ["props"] = (node, args) => node
                    .SelectSingleNode(@"//div[@class='description']")
                    .InnerText,
                ["Цена"] = (node, args) => node
                    .SelectSingleNode(@"//span[@itemprop='price']")
                    .InnerText.Replace("р.", string.Empty).Replace(" ", string.Empty),
                ["Валюта"] = (node, o) => "RUB",
                [@"""Доступен для заказа"""] = (node, o) => "1",
                [@"Статус"] = (node, o) => "1",
                ["Описание"] = (node, args) => string.Join(".", node
                        .SelectSingleNode(@".//div[@id='tab-description']")
                        .InnerText.Split(new[] {'.', '!', '?'})
                        .Where(x => !x.Contains("Пластмастер"))),

            };
            singlePropertiesProduct[@"""Код артикула"""] = (node, args) =>
                Regex.Match(singlePropertiesProduct["props"](node, args),
                        @"Артикул:\s*(\w+)")
                    .Groups[1].Value;
            singlePropertiesProduct[@"Размеры"] = (node, args) =>
            {
                var text = singlePropertiesProduct["props"](node, args);
                var result = Regex.Match(singlePropertiesProduct["props"](node, args),
                    @"Длина:\s+([\w\.]+\s*мм).*Ширина:\s+([\w\.]+\s*мм).*Высота:\s+([\w\.]+\s*мм)",
                    RegexOptions.Singleline).Groups;
                return $"{result[1]} x {result[2]} x {result[3]}";
            };
            singlePropertiesProduct["Заголовок"] =
                (node, args) => singlePropertiesProduct["Наименование"](node, args);
            singlePropertiesProduct[@"""Ссылка на витрину"""] = (node, args) =>
                Humanization.GetHumanLink(singlePropertiesProduct["Наименование"](node, args));

            var parser = new LiquiMolyClass(
                isCategory: node => node
                    ._SelectNodes(@"//b[contains(text(), 'Сортировка')]")
                    .Count != 0,
                findProducts: (node, args) => node
                    ._SelectNodes(@"//*[@id='content']//div[@class='name']/a")
                    .Select(x => new ArgumentObject(x.Attributes["href"].Value))
                    .ToArray(),
                singlePropertiesCategory: new Dictionary<string, Search<string>>
                {
                    ["Наименование"] = (node, args) => new string('!', (int) args.Args[0]) + node
                                   .SelectSingleNode(@".//*[@id='column-left']//a[@class='active']")
                                   .InnerText
                },
                pluralPropertiesProduct: new Dictionary<string, Search<string[]>>
                {
                    ["Изображения"] = (node, args) =>
                    {
                        var a = node
                            ._SelectNodes(@"//a[contains(@id, 'zoom')]")
                            .Select(x => x.Attributes["href"].Value)
                            .ToArray();
                        var result = node
                            ._SelectNodes(@"//div[@class='zoom-top']/a")
                            .Select(x => x.Attributes["href"].Value)
                            .ToArray();
                        return a.Extend(result).ToArray();
                    }
                },
                singlePropertiesProduct: singlePropertiesProduct
                );
            var arguments = new[]
                {
                    @"https://playdorado.ru/piramidy/",
                    @"https://playdorado.ru/katalki/",
                    @"https://playdorado.ru/razvivaushie/",
                    @"https://playdorado.ru/igrovye/",
                    @"https://playdorado.ru/kukly/",
                    @"https://playdorado.ru/posuda/",
                    @"https://playdorado.ru/transport/",
                    @"https://playdorado.ru/sport/",
                    @"https://playdorado.ru/sezonnye/",
                    @"https://playdorado.ru/pvcl/",
                    @"https://playdorado.ru/licenzionnaya-produkciya/"
                }
                .Select(x => new ArgumentObject(url: x + suffix,
                    args: new object[] { 2 }));
            var collection = parser.GetProductOrCategory(arguments);
            collection = new[]
            {
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "Temporary"}, isCategory: true),
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "!Playdorado"}, isCategory: true)
            }.Extend(collection);
            Import.Write(path: @"..\..\..\CSV\playdorado.csv",
                collection: collection.ToArray(),
                headers: Constants.WebAsystKeys,
                format: Constants.WebAsystFormatter);
        }
    }
}