using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataParser.DataExtractorExamples;
using DataParser.HelperClasses;

namespace DataParser.ParserExamples
{
    public class PolisieToysExample
    {
        

        public static void Parse()
        {
            var mainUrl = @"http://www.polesie-toys.com/";
            var addedUrl = @"http://www.polesie-toys.com/cat/";

            var singlePropertiesProduct = new Dictionary<string, Search<string>>
            {
                ["Наименование"] = (node, args) => node
                    .SelectSingleNode(@"//h1")
                    .InnerText,
                [@"""Код артикула"""] = (node, args) => node
                    .SelectSingleNode(@"//*[@class='catalog-item-fields-table']//tr[1]/td[2]")
                    .InnerText.Trim(),
                ["Описание"] = (node, args) => node
                    .SelectSingleNode(@".//*[@id='catalog_item_view_tabs_item_0']")
                    ?.InnerText ?? string.Empty + 
                    node.SelectSingleNode(@"//*[@class='catalog-item-fields-table']")
                    .InnerHtml,
                ["Валюта"] = (node, o) => "RUB",
                [@"""Доступен для заказа"""] = (node, o) => "1",
                [@"Статус"] = (node, o) => "1",
            };

            singlePropertiesProduct["Заголовок"] =
                (node, args) => singlePropertiesProduct["Наименование"](node, args);
            singlePropertiesProduct[@"""Ссылка на витрину"""] = (node, args) =>
                Humanization.GetHumanLink(singlePropertiesProduct["Наименование"](node, args));
            singlePropertiesProduct[@"""Краткое описание"""] =
                (node, args) => singlePropertiesProduct["Описание"](node, args).Substring(0, 100) + "...";

            var parser = new LiquiMolyClass(
                isCategory: node => node
                    ._SelectNodes(@"//*[@class='catalog-item-fields-table']")
                    .Count == 0,
                findSubcatalogs: (node, args) => node
                    ._SelectNodes(@"//*[@class='catalog-categories-div']/div[2]/a[not (contains (text(), 'Сервисное'))]")
                    .Select(x => new ArgumentObject(x.Attributes["href"].Value, new[] { (object)((int)args.Args[0] + 1) }))
                    .ToArray(),
                findProducts: (node, args) => node
                    ._SelectNodes(@"//*[@class='catalog-items-div']/div[3]/a")
                    .Select(x => new ArgumentObject(x.Attributes["href"].Value))
                    .ToArray(),
                singlePropertiesCategory: new Dictionary<string, Search<string>>
                    {
                        ["Наименование"] = (node, args) => {
                            var a = new string('!', (int)args.Args[0]) + node
.SelectSingleNode(@"//h1").InnerText;

                            return a;
                            }
                    },
                singlePropertiesProduct: singlePropertiesProduct,
                pluralPropertiesProduct: new Dictionary<string, Search<string[]>>
                    {
                        ["Изображения"] = (node, args) => node
                            ._SelectNodes(@"//*[@class='img-thumbnail']")
                            .Select(x => x.Attributes["src"].Value)
                            .ToArray()
                    },
                encoding: Encoding.UTF8
                );

            var arguments = new[] { "yelektromobili", "katalki", "konstruktory",
                "transport", "voennaya-tehnika", "razvivayushhie_igrushki",
                "igrovye_kompleksy", "letnij_assortiment", "mebel-dlya-kukol",
                "posuda_dlya_kukol", "nabory_produktov", "produkciya_v_displeyah",
                "tovary_hozyajstvenno_bytovogo_naznacheniya"}
                .Select(x => new ArgumentObject(url: addedUrl + x,
                                        args: new object[] { 2 }));

            var collection = parser.GetProductOrCategory(arguments);
            collection = new[]
            {
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "Temporary"}, isCategory: true),
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "!PolisieToys"}, isCategory: true)
            }.Extend(collection);

            Import.Write(path: "../../../CSV/polisieToys.csv",
               collection: collection.ToArray(),
               headers: Constants.WebAsystKeys,
               format: Constants.WebAsystFormatter);
        }
    }
}