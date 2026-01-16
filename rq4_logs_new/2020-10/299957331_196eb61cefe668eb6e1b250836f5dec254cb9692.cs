using System;
using System.Collections.Generic;

namespace TravellingSalespersonProj
{
    public static class DataDisplay
    {
        public static void PrintGraphOfNodes(Graph graph)
        {
            foreach (KeyValuePair<int, float[]> entry in graph.GraphOfNodes)
            {
                Console.WriteLine($"{System.Environment.NewLine}Node ID: {entry.Key}, Node Coordinates: {entry.Value[0]}, {entry.Value[1]}");
            }
        }

        public static void PrintRouteAndCalculation(int[] route, float costOfRoute)
        {
            string routePath = String.Empty;

            foreach (int nodeId in route)
            {
                routePath = $"{routePath} {nodeId.ToString()}";
            }

            Console.WriteLine($"{System.Environment.NewLine}The route taken was {routePath} with a cost of {costOfRoute}");
        }
    }
}