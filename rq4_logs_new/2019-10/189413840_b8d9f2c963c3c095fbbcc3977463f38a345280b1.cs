using GacExplorer.Services.DTO;
using System;
using System.Collections.Generic;

namespace GacExplorer.Services.OperationResults
{
    public class GacutilOutputParserResult : ServiceOperationResult
    {
        public List<AssemblyLineDto> AssemblyLines { get; set; }

        public GacutilOutputParserResult(OperationResult result, string message = "") : base(result, message)
        {
        }

        public GacutilOutputParserResult(OperationResult result, string message, Exception exception) : base(result, message, exception)
        {
        }
    }
}