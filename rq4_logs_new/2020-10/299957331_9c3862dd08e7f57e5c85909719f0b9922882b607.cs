using System;
using System.Collections.Generic;
using System.Text;

namespace TravellingSalespersonProj.GenericTSP
{
    public class TourFormatter
    {
        public int[] AddStartAndEndNodeToRoute(int startAndEndNodeId, int[] route)
        {
            int[] completeRoute = new int[route.Length + 2];
            completeRoute[0] = startAndEndNodeId;
            Array.Copy(route, 0, completeRoute, 1, route.Length);
            completeRoute[completeRoute.Length - 1] = startAndEndNodeId;

            return completeRoute;
        }

        public int[] RemoveStartAndEndNodeToRoute(int[] route)
        {
            int[] formattedRoute = new int[route.Length - 2];
            Array.Copy(route, 1, formattedRoute, 0, route.Length - 2);

            return formattedRoute;
        }
    }
}