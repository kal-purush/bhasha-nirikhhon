using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpenXMLXLSXImporter.FileAccess
{
    public interface IXlsxSheetFile
    {
        bool TryGetRow(uint desiredRowIndex, out IEnumerator<Cell> cellEnumerator);
    }

    public interface IXlsxSheetFilePromise
    {
        Task<IXlsxSheetFile> GetLoadedFile();
    }

    public class XlsxSheetFile : IXlsxSheetFile, IXlsxSheetFilePromise
    {
        private IXlsxDocumentFilePromise _fileAccessPromise;

        private string _sheetName;

        private Sheet _sheet;
        private WorksheetPart _workbookPart;
        private Worksheet _worksheet;
        private SheetData _sheetData;

        private Task _loadSpreadSheetData;

        private IEnumerable<Row> _rowsEnumerable;
        private IEnumerator<Row> _rowEnumerator;
        private IEnumerable<Cell> _cellEnumerable;
        private Row _row;
        private bool _rowsLoadedIn ;

        private UInt32Value _rowIndexNullable;
        private uint _rowIndex;

        private IDictionary<uint, IEnumerator<Cell>> _rows;

        public XlsxSheetFile(IXlsxDocumentFilePromise fileAccess, string sheetName)
        {
            _fileAccessPromise = fileAccess;
            _sheetName = sheetName;
            _rowsLoadedIn = false;
            _loadSpreadSheetData = Task.Run(LoadSpreadSheetData);
        }

        protected async Task LoadSpreadSheetData()
        {
            _rows = new Dictionary<uint, IEnumerator<Cell>>();
            IXlsxDocumentFile fileAccess = await _fileAccessPromise.GetLoadedFile();
            _sheet = fileAccess.GetSheet(_sheetName);
            _workbookPart = fileAccess.WorkbookPart.GetPartById(_sheet.Id) as WorksheetPart;
            _worksheet = _workbookPart.Worksheet;
            _sheetData = _worksheet.Elements<SheetData>().First();
           _rowsEnumerable = _sheetData.Elements<Row>();
            _rowEnumerator = _rowsEnumerable.GetEnumerator();
        }

        public async Task<IXlsxSheetFile> GetLoadedFile()
        {
            await _loadSpreadSheetData;
            return this;
        }

        bool IXlsxSheetFile.TryGetRow(uint desiredRowIndex, out IEnumerator<Cell> cellEnumerator)
        {
            //this will only load in up to the row we need
            //if we try loading in a row that does not exist then we will load all of them in
            //I'm assuming that data from here might still be in the file and we don't want to read things we don't need
            while (!_rowsLoadedIn && !_rows.ContainsKey(desiredRowIndex))
            {
                if (_rowEnumerator.MoveNext())
                {
                    _row = _rowEnumerator.Current;
                    _rowIndexNullable = _row.RowIndex;
                    if (_rowIndexNullable.HasValue)
                    {
                        _rowIndex = _rowIndexNullable.Value;
                        _cellEnumerable = _row.Elements<Cell>();
                        cellEnumerator = _cellEnumerable.GetEnumerator();
                        _rows.Add(_rowIndex, cellEnumerator);
                        if (_rowIndex == desiredRowIndex)
                            return true;
                    }
                }
                else
                {
                    _rowsLoadedIn = true;
                    break;
                }
            }
            cellEnumerator = null;
            return false;
        }
    }
}