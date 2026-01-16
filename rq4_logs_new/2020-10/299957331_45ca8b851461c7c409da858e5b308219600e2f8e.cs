using System;
using System.Collections.Generic;
using System.Text;

namespace TravellingSalespersonProj
{
    class DefaultGraphGenerator
    {
        public DefaultGraphGenerator()
        {

        }

        public List<Node> GenerateNodes()
        {
            List<Node> nodes = new List<Node>();

            for(int index = 0; index < 5; index++)
            {
                string nodeLetter = ConvertIntToString.Convert(index);
                nodes.Add(new Node(nodeLetter));
            }

            foreach(Node node in nodes)
            {
                Console.WriteLine(node.NodeLetter);
            }

            return nodes;
        }
    }
}