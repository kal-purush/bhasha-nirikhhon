using System;
using System.Linq;

namespace TravellingSalespersonProj.SecondAttempt
{
    public class RandomRouteGenerator
    {
        public int[] GenerateSingleRandomRoute(int numberOfNodes, int startingNode)
        {
            // Will this actually be random if always newed up here?
            Random rnd = new Random();

            int[] nodesToVisit = new int[numberOfNodes - 1];
            int count = 0;

            // Makes an array with the nodes to visit that are not the start nodes
            for (int index = 0; index < numberOfNodes - 1; index++)
            {
                if(count == startingNode)
                {
                    count++;
                }

                nodesToVisit[index] = count;
                count++;
            }

            // Randomises the array to generate a random order of nodes to visit
            nodesToVisit = nodesToVisit.OrderBy(x => rnd.Next()).ToArray();

            // Copies the nodes to visit to a new array withe the starting node added to the beinning and end of the array
            int[] finalRoute = new int[numberOfNodes + 1];

            finalRoute[0] = startingNode;
            finalRoute[finalRoute.Length -1] = startingNode;
            nodesToVisit.CopyTo(finalRoute, 1);

            return finalRoute;
        }
    }
}