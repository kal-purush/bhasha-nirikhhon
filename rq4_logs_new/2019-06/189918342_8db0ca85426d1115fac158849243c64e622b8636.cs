using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DrAnalyzer.Analyzer.Info
{
    public class NotGatheredError : IGatheredInfo
    {
        public GatherType Type { get; } = GatherType.GatherDeactivated;

        public GatherFuncType FuncType { get; } = GatherFuncType.GatherUnknownFunc;

        public string Name { get; } = "Connection with the process lost";

        public object AsObject()
        {
            throw new NotImplementedException();
        }

        public string AsTextMessage()
        {
            return String.Format("{0} ({1})", this.Type.GetDescription(), this.FuncType.GetDescription());
        }
    }
}