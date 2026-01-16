using Nito.AsyncEx;
using OpenXMLXLSXImporter.Builders;
using OpenXMLXLSXImporter.CellData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OpenXMLXLSXImporter
{
    public interface IDefaultSheetInstructionBuilder
    {
        IDefaultSheetInstructionBuilder LoadSingleCell(uint row, string cell);
    }
    public class SpreadSheetInstructionBuilderManager : ISpreadSheetInstructionBuilderManager, IDefaultSheetInstructionBuilder
    {
        private string _sheet;
        private List<ISpreadSheetInstructionKey> _keys;
        private ISpreadSheetInstructionBuilder _builder;

        private Action<IDefaultSheetInstructionBuilder> _execute;

        private IEnumerable<Task<IEnumerable<Task<ICellData>>>> _result;//lol
        public SpreadSheetInstructionBuilderManager(string sheet,Action<IDefaultSheetInstructionBuilder> ac)
        {
  
            _sheet = sheet;
            _keys = new List<ISpreadSheetInstructionKey>();
            _builder = null;
            _execute = ac;
            
        }
        string ISpreadSheetInstructionBuilderManager.Sheet => _sheet;

        public async IAsyncEnumerable<IEnumerable<Task<ICellData>>> GetResults()
        {
            foreach(Task<IEnumerable<Task<ICellData>>> k in _result)
                yield return await k;
        }

        IDefaultSheetInstructionBuilder IDefaultSheetInstructionBuilder.LoadSingleCell(uint row, string cell)
        {
            _keys.Add(_builder.LoadSingleCell(row, cell));
            return this;
        }

        void ISpreadSheetInstructionBuilderManager.LoadConfig(ISpreadSheetInstructionBuilder builder)
        {
            _builder = builder;
            _execute(this);
        }

        async Task ISpreadSheetInstructionBuilderManager.ResultsProcessed(ISpreadSheetQueryResults query)
        {
            _result = _keys.Select(x=>query.GetResults(x)).ToList();
        }
    }
}