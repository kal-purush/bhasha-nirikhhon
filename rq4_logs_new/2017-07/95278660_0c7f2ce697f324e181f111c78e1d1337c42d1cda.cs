using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataParser.HelperClasses;
using Microsoft.Office.Interop.Excel;

namespace DataParser.DataExtractorExamples
{
    class IgrRuDataExtractorExample
    {
        public static IEnumerable<ProductCategoryObject> Extract()
        {
            return DataExtractor.Extract(
                path: @"D:\Загрузки\IgrRu.xlsx",
                filter: row => row.Count(s => s.Value2 != null) >= 5,
                pagesToExtact: new HashSet<int> { 1 },
                singlePropertiesFunc: new Dictionary<string, Func<Range[], string>>
                {
                    ["Наименование"] = row => row[3]._Value2(),
                    [@"""Код артикула"""] = row => row[1]._Value2(),
                    ["Видеоролик"] = row => row[13]._Hyperlink(1),
                    ["Цена"] = row => row[8]._Value2(),
                    ["Код"] = row => row[0]._Value2()
                },
                pluralPropertiesFunc: new Dictionary<string, Func<Range[], string[]>>(),
                startIndex: 16);
        }
    }
}