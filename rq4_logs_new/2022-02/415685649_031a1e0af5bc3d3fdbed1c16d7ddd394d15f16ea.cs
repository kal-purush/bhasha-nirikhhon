using DocumentFormat.OpenXml.Spreadsheet;
using Nito.AsyncEx;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpenXMLXLXSImporter.CellData
{
    public class DeferredCell : IFutureCell
    {
        private Cell _deferredCell;
        private AsyncManualResetEvent _mre;
        public DeferredCell(uint cellRowIndex, string cellColumnIndex, Cell cell)
        {
            CellRowIndex = cellRowIndex;
            CellColumnIndex = cellColumnIndex;
            _deferredCell = cell;
        }
        public string CellColumnIndex { get; set; }
        public uint CellRowIndex { get; set; }

        public Task<ICellData> GetData()
        {
            throw new NotImplementedException();
        }
    }
}