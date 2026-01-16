using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using OpenXMLXLXSImporter.CellData;
using OpenXMLXLXSImporter.ExcelGrid;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpenXMLXLXSImporter.FileAccess
{
    public class SpreadSheetFile : ISpreadSheetFilePromise, ISpreadSheetFile
    {
        private Stream _stream;
        //The Sheet
        private SpreadsheetDocument _spreadsheet;
        //WorkBookParts
        private WorkbookPart _workbookPart;
        private SharedStringTablePart _sharedStringTablePart;
        private SharedStringTable _sharedStringTable;
        private WorkbookStylesPart _workBookStyleParts;
        private Stylesheet _styleSheet;
        private CellFormats _cellFormats;
        //The Task that Loads in the SpreadSheetDocumentData
        private Task _loadSpreadSheetData;

        private Dictionary<string, SpreadSheetGrid> _loadedSheets;
        private Dictionary<string, Sheet> _sheetRef;

        public SpreadSheetFile(Stream stream)
        {
            _stream = stream;
            _loadSpreadSheetData = LoadSpreadSheetDocuemntData();
            _loadedSheets = new Dictionary<string, SpreadSheetGrid>();
        }

        async Task<ISpreadSheetFile> ISpreadSheetFilePromise.GetLoadedFile()
        {
            await _loadSpreadSheetData;//Ensure this is loaded first
            return this;
        }

        Sheet ISpreadSheetFile.GetSheet(string sheetName) => _sheetRef[sheetName];

        WorkbookPart ISpreadSheetFile.WorkbookPart => _workbookPart;

        /// <summary>
        /// Common Reusable Parts of the WorkSheet
        /// </summary>
        /// <returns></returns>
        private Task LoadSpreadSheetDocuemntData()
        {
            return Task.Run(() =>
            {
                _spreadsheet = SpreadsheetDocument.Open(_stream, false, new OpenSettings { AutoSave = false });
                _workbookPart = _spreadsheet.WorkbookPart;

                _sharedStringTablePart = _workbookPart.GetPartsOfType<SharedStringTablePart>().First();
                _sharedStringTable = _sharedStringTablePart.SharedStringTable;

                _workBookStyleParts = _workbookPart.WorkbookStylesPart;
                _styleSheet = _workBookStyleParts.Stylesheet;
                _cellFormats = _styleSheet.CellFormats;

                //this will get all the sheetNames
                _sheetRef = _workbookPart.Workbook.Sheets.Cast<Sheet>().Where(x => x.Name.HasValue).ToDictionary(x => x.Name.Value);
            });
        }


        /// <summary>
        /// This is for custom Cell Types like dates Cell with relations like text etc
        /// </summary>
        /// <param name="c"></param>
        /// <param name="cellData"></param>
        /// <returns></returns>
        public bool ProcessCustomCell(Cell c, out ICellData cellData)
        {
            if (c.StyleIndex != null)
            {
                int index = int.Parse(c.StyleIndex.InnerText);
                CellFormat cellFormat = _cellFormats.ChildElements[index] as CellFormat;
                if (cellFormat != null)
                {
                    if (ExcelStaticData.DATE_FROMAT_DICTIONARY.ContainsKey(cellFormat.NumberFormatId))
                    {
                        if (!string.IsNullOrEmpty(c.CellValue.Text))
                        {
                            if (double.TryParse(c.CellValue.Text, out double cellDouble))
                            {

                                DateTime theDate = DateTime.FromOADate(cellDouble);
                                cellData = new CellDataDate { Date = theDate, DateFormat = ExcelStaticData.DATE_FROMAT_DICTIONARY[cellFormat.NumberFormatId] };
                                return true;
                            }
                        }
                    }
                }
            }

            if (c.DataType?.Value != null && c.DataType?.Value == CellValues.SharedString)
            {
                cellData = new CellDataRelation(int.Parse(c.CellValue.InnerText), _sharedStringTable);
                return true;
            }
            cellData = null;
            return false;
        }

        public static string GetColumnIndexByColumnReference(StringValue columnReference)
        {
            string v = columnReference.Value;
            for (int i = v.Length - 1; i >= 0; i--)
            {
                char c = v[i];
                if (!Char.IsNumber(c))
                {
                    return v.Substring(0, i + 1);
                }

            }
            return v;
        }

        /// <summary>
        /// this will be called for each work sheet and will process each cell
        /// </summary>
        /// <param name="worksheetPart"></param>
        /// <param name="grid"></param>
        /// <returns></returns>
        //protected async Task ProcessWorkSheet(WorksheetPart worksheetPart, SpreadSheetGrid grid)
        //{
        //    await Task.Run(() => {
        //        List<Task> cellTask = new List<Task>();
        //        Worksheet ws = worksheetPart.Worksheet;
        //        SheetData sheetData = ws.Elements<SheetData>().
        //        IEnumerator<Row> enumerator = sheetData.Elements<Row>().GetEnumerator();
        //        while (enumerator.MoveNext())
        //        {
        //            Row r = enumerator.Current;
        //            IEnumerable<Cell> cells = r.Elements<Cell>();

        //            foreach (Cell c in cells)
        //            {
        //                if (c?.CellValue != null)
        //                {
        //                    ICellData cellData;
        //                    bool hasBeenProcessed = ProcessCustomCell(c, out cellData);
        //                    if (!hasBeenProcessed)
        //                    {
        //                        cellData = new CellDataContent { Text = c.CellValue.Text };
        //                    }

        //                    if (cellData != null)
        //                    {
        //                        cellData.CellColumnIndex = GetColumnIndexByColumnReference(c.CellReference);
        //                        cellData.CellRowIndex = r.RowIndex.Value;
        //                        cellTask.Add(grid.Add(cellData));
        //                    }
        //                }
        //            }
        //        }
        //        cellTask.ForEach(x => x.Wait());
        //        grid.FinishedLoading();
        //    });

        //}

        public async Task<SpreadSheetGrid> LoadSpreadSheetData(ISheetProperties sheet)
        {
            await _loadSpreadSheetData;
            if (_sheetRef.ContainsKey(sheet.Sheet))
            {
                if (!_loadedSheets.ContainsKey(sheet.Sheet))
                {
                    //this is the first time we use this sheet
                    _loadedSheets[sheet.Sheet] = new SpreadSheetGrid(this, sheet);
                }
                return _loadedSheets[sheet.Sheet];
            }
            return null;
        }

        public void Dispose()
        {
            //if(_loadedSheets != null)
            //{
            //    foreach(Task t in _loadedSheets)
            //        t?.Wait();
            //}
            _spreadsheet.Dispose();
        }
    }
}