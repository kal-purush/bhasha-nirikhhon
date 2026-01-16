using System;
using System.Collections.Generic;
using System.Text;

namespace TravellingSalespersonProj.SecondAttempt
{
    public class Graph
    {
        // Stores the graph in the format: NodeID, [NodeXPos, NodeYPos]
        public Dictionary<int, float[]> GraphOfNodes { get; set; }

        public Graph()
        {
            GraphOfNodes = new Dictionary<int, float[]>();
        }

        public void RunDefaultGraphSetUp()
        {
            PopulateGraphWithDefaultValues();
            PrintGraphOfNodes();
        }

        // Add a check to see if the graph already contais that node?
        public void AddNodeToGraph(int nodeID, float[] nodeCoordinates)
        {
            GraphOfNodes.Add(nodeID, nodeCoordinates);
        }

        public void PopulateGraphWithDefaultValues()
        {
            // Node node 0 (A) had coordinate 0,0 with y traveling downwards with positive values
            // Note this isn't exactly the example
            float[,] defaultGraph =  new float[,]
            {
                { 0, 0, 0 },
                { 1, 20, 0 },
                { 2, 0, 42 },
                { 3, 12, 42 }
            };

            for(int index = 0; index < defaultGraph.GetLength(0); index++)
            {
                float[] coordinates = new float[] { defaultGraph[index, 1], defaultGraph[index, 2] };
                AddNodeToGraph(Convert.ToInt32(defaultGraph[index, 0]), coordinates);
            }
        }

        public void PrintGraphOfNodes()
        {
            foreach (KeyValuePair<int, float[]> entry in GraphOfNodes)
            {
                Console.WriteLine($"Node ID: {entry.Key}, Node Coordinates: {entry.Value[0]}, {entry.Value[1]}{System.Environment.NewLine}");
            }
        }
    }
}