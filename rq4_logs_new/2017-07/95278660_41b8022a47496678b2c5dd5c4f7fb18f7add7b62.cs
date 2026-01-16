using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataParser.HelperClasses;
using MySql.Data.MySqlClient;

namespace DataParser
{
    class StihlExampleCategory
    {
        public static void Parse()
        {
            var url = @"http://xn----itbwjj6a.xn--p1ai";
            var connection = new DbConnector("root", "n1k1t0s28mysql", "192.168.0.104", "u24223");
            var collection = connection.GetProductCategoryObjects(
                query: @"SELECT name, code, text, comment, img " +
                       @"FROM mp_catalog_articles",
                
                singlePropertiesCategoryFunc: new Dictionary<string, Func<MySqlDataReader, string>>
                {
                    ["Наименование"] = reader => '!' + reader.GetString(0).Trim(),
                    [@"""Ссылка на витрину"""] = reader => Humanization.GetHumanLink(reader.GetString(0)),
                    ["Описание"] = reader => reader.GetString(2),
                    [@"""Краткое описание"""] = reader => reader.GetString(3),
                });
            collection = new[]
            {
                new ProductCategoryObject(
                    new Dictionary<string, string> {["Наименование"] = "spec-m"}, isCategory: true),
            }.Extend(collection);
            Import.Write(path: "../../../CSV/spec-m-category.csv",
                collection: collection
                    .Distinct(new ProductEqualityComparer(o => o.SingleProperties["Наименование"]))
                    .ToArray(),
                headers: Constants.WebAsystKeys,
                format: Constants.WebAsystFormatter);
        }
    }
}