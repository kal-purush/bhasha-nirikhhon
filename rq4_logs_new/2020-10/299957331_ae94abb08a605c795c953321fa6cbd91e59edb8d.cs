using System;
using System.Collections.Generic;

namespace TravellingSalespersonProj
{
    public class GraphGenerator
    {

        public GraphGenerator(bool isUsingDefaultGraph)
        {
        }

        public Graph GenerateGraph()
        {
            Graph graph = new Graph();

            graph.NodesInCity = GenerateAllNodes();

            foreach (KeyValuePair<string, Node> entry in graph.NodesInCity)
            {
                GenerateEdgesForCurrentNode(entry.Key, entry.Value);
            }

            return graph;
        }

        public Dictionary<string, Node> GenerateAllNodes()
        {
            Dictionary<string, Node> nodes = new Dictionary<string, Node>();

            for (int index = 1; index < 5; index++)
            {
                string nodeLetter = ConvertIntToString.Convert(index);
                nodes.Add(nodeLetter, new Node());
            }
            return nodes;
        }

        public Dictionary<string, int> GenerateEdgesForCurrentNode(string startingNodeLetter, Node currentNode)
        {
            string[,] defaultEdges = GetEdgesWithWeightValues();

            Dictionary<string, int> edges = new Dictionary<string, int>();

            for (int index = 0; index < defaultEdges.GetLength(0); index++)
            {
                if(defaultEdges[index, 0].Equals(startingNodeLetter))
                {
                    currentNode.Edges.Add(defaultEdges[index, 1], ConvertStringToInt(defaultEdges[index, 2]));
                }
            }

            return edges;
        }

        private int ConvertStringToInt(string stringToConvert)
        {
            try
            {
                int numVal = Int32.Parse(stringToConvert);
                return numVal;
            }
            catch (FormatException e)
            {
                Console.WriteLine(e.Message);
                return 0;
            }
        }

        private string[,] GetEdgesWithWeightValues()
        {
            return new string[,]
            {
                { "A", "B", "20" },
                { "A", "C", "42" },
                { "A", "D", "35" },
                { "B", "A", "20" },
                { "B", "C", "30" },
                { "B", "D", "34" },
                { "C", "A", "42" },
                { "C", "B", "30" },
                { "C", "D", "12" },
                { "D", "A", "35" },
                { "D", "B", "34" },
                { "D", "C", "12" },
            };
        }
    }
}