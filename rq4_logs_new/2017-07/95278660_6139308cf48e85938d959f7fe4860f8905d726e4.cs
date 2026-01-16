using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataParser.Examples
{
    class AddinolDataExtractorExample
    {
        public static IEnumerable<ProductCategoryObject> Extract()
        {
            return DataExtractor.Extract(
                path: @"D:\GitHub\ShopParser\DataParser\bin\Debug\Addinol.xls",
                filter: row => row.Count(s => !s.Equals(String.Empty)) > 1,
                pagesToExtact: new HashSet<int> {1},
                singlePropertiesFunc: new Dictionary<string, Func<object[], string>>
                {
                    ["Наименование"] = row => $"{row[0].ToString().Substring(8)} {row[1]}",
                    ["SAE"] = row => row[2].ToString(),
                    ["ACEA"] = row => row[3].ToString(),
                    ["API"] = row => row[4].ToString(),
                    ["Объем"] = row => $"{row[5].ToString()} {row[6]}",
                    ["Цена"] = row => row[7].ToString(),
                },
                pluralPropertiesFunc: new Dictionary<string, Func<object[], string[]>>(),
                startIndex: 9);
        }
    }
}