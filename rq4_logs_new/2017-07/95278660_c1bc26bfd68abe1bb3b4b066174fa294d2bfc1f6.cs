using System;
using System.Collections.Generic;
using System.Linq;
using Excel = Microsoft.Office.Interop.Excel;

namespace DataParser
{
    class DataExtractor
    {
        public static IEnumerable<ProductCategoryObject> Extract(string path,
            HashSet<int> pagesToExtact,
            Dictionary<string, Func<string[], string>> singlePropertiesFunc,
            Dictionary<string, Func<string[], string[]>> pluralPropertiesFunc)
        { 
            var xlApp = new Excel.Application();
            var xlWorkBook = xlApp.Workbooks.Open(Filename: path, ReadOnly: true);
            foreach (var index in pagesToExtact)
            {
                var xlWorksheet = (Excel.Worksheet)xlWorkBook.Worksheets.Item[index];
                var range = xlWorksheet.UsedRange;
                for (int row = 1; row <= range.Rows.Count; row++)
                {
                    var rowArray = Enumerable
                        .Range(1, range.Columns.Count)
                        .Select(x => (string) (range.Cells[row, x] as Excel.Range)?.Value2
                                     ?? String.Empty)
                        .ToArray();
                    var singleProperties = new Dictionary<string, string>();
                    var pluralProperties = new Dictionary<string, string[]>();
                    foreach (var func in singlePropertiesFunc)
                    {
                        singleProperties[func.Key] = func.Value(rowArray);
                    }
                    foreach (var func in pluralPropertiesFunc)
                    {
                        pluralProperties[func.Key] = func.Value(rowArray);
                    }

                    yield return new ProductCategoryObject(singleProperties, 
                        pluralProperties);
                }
            }
        }
    }
}