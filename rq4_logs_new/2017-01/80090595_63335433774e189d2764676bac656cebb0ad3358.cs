using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PVCSClient.Libraries
{
    public class PVCSFileGroupCollection : Collection<PVCSFileGroup>
    {
        public PVCSFileGroupCollection() : base() { }
        public PVCSFileGroupCollection(IEnumerable<string> lines) : this()
        {
            foreach (string curLine in lines)
                if (!string.IsNullOrWhiteSpace(curLine))
                    this.Add(new Libraries.PVCSFileGroup(curLine));
        }
        public PVCSFileGroupCollection(string input) : this(input.Split(new[] { '\r', '\n' })) { }

    }
}