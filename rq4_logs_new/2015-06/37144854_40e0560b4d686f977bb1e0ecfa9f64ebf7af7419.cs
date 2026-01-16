using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DesignPatterns.BehavioralPatterns.State.DephtFirstSearch
{
    public class Black : Color
    {
        public void TookOver(Node node, List<Node> nodes)
        {
            nodes.Add(node);
        }
    }
}