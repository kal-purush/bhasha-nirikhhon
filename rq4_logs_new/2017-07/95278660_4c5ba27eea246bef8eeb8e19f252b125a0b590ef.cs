using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataParser.HelperClasses;

namespace DataParser.Examples
{
    class IgrRuExample
    {
        public static void Parse()
        {
            var mainUrl = @"http://igr.ru/";
            var addedUrl = @"http://igr.ru/cat.php?pgsize=1000&sort=1&pgsort=1&days=9000&rub=207&prub=";
            var parser = new LiquiMolyClass(
                isCategory: node => node
                    ._SelectNodes(@"//*[@class='good_good']")
                    .Count != 0,
                findProducts: (node, args) =>node
                        ._SelectNodes(@"//*[@class='good_good']//a[contains(text(), 'Подробнее')]")
                        .Select(x => new ArgumentObject(mainUrl + x.Attributes["href"].Value))
                        .ToArray(),
                singlePropertiesCategory: new Dictionary<string, Search<string>>
                {
                    ["Наименование"] = (node, args) => node
                        .SelectSingleNode(@"//img[@alt]/following-sibling::a[1]")
                        .InnerText,
                },
                singlePropertiesProduct: new Dictionary<string, Search<string>>
                {
                    ["Наименование"] = (node, args) => node
                        .SelectSingleNode(@"//h2")
                        .InnerText,
                    [@"""Код артикула"""] = (node, args) => node
                            .SelectSingleNode(@"//*[contains(text(), 'Код')]/../text()[1]")
                            .InnerText,
                    ["Описание"] = (node, args) => node
                        .SelectSingleNode(@"//p[@class='textsm' and preceding-sibling::table]")
                        ?.InnerHtml ?? String.Empty,
                },
                pluralPropertiesProduct: new Dictionary<string, Search<string[]>>
                {
                    ["Изображения"] = (node, args) => node
                        ._SelectNodes(@"//td[@class='textsm']/a[img]")
                        .Select(x => mainUrl + x.Attributes["href"].Value)
                        .ToArray()
                },
                encoding: Encoding.Default
                );
            var arguments = new[] {651,} //382, 385, 387, 827, 678, 680, 672, 208, 677, 682,}
                .Select(x => new ArgumentObject(url: addedUrl + x,
                                        args: new object[] {0}));
            var collection = parser.GetProductOrCategory(arguments);
            Import.Write(path: "igrRu.csv"
                , collection: collection
                , headers: Constants.WebAsystKeys
                , format: Constants.WebAsystFormatter);

        }
    }
}