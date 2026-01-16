using System;
using System.Collections.Generic;
using System.Linq;
using DataParser.HelperClasses;
using Microsoft.Office.Interop.Excel;

namespace DataParser.DataExtractorExamples
{
    class PolisieToysDataExtractorExample
    {
        public static IEnumerable<ProductCategoryObject> Extract()
        {
            return DataExtractor.Extract(
                path: @"D:\GitHub\ShopParser\CSV\polisie.xlsx",
                filter: row => row[0]._Value2() != null,
                pagesToExtact: new HashSet<int> { 2 },
                singlePropertiesFunc: new Dictionary<string, Func<Range[], string>>
                {
                    ["Наименование"] = row => row[2]._Value2(),
                    ["Ариикул"] = row => row[1]._Value2(),
                    ["Цена"] = row => row[7]._Value2(),
                },
                pluralPropertiesFunc: new Dictionary<string, Func<Range[], string[]>>(),
                startIndex: 12);
        }
    }
}