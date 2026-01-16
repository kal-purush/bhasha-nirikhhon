using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ghosen.CAN;

namespace ghosen.Plugins
{
    public class CandumpFilePlugin : ghosen.IPluginV1
    {
        public string Name => "CandumpFilePlugin";

        public IEnumerable<string> SupportedFormats => new string[] { "Candump file dump" };

        public CAN.Message ParseLine(string line)
        {
            return CandumpLine.Parse(line).Message;
        }

        public IEnumerable<Message> ParseLines(IEnumerable<string> lines)
        {
            foreach (var line in lines)
            {
                var tmp = CandumpLine.Parse(line).Message;
                if (tmp.ArbId == 0x7E0 ||
                    tmp.ArbId == 0x7E8)
                {
                    yield return tmp;
                }
            }
            yield break;
        }
    }
}