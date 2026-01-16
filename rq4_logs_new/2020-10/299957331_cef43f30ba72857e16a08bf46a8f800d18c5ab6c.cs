using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace TravellingSalespersonProj.LocalSearch
{
    public class TimeBasedEvaluator
    {
        private readonly RandomRouteGenerator randomRouteGenerator;
        private readonly RouteEvaluator routeEvaluator;

        public TimeBasedEvaluator()
        {
            this.randomRouteGenerator = new RandomRouteGenerator();
            this.routeEvaluator = new RouteEvaluator();
        }

        public KeyValuePair<int[], float> CalculateBestRandomRouteInGivenTime(Graph graph, int timeToExecuteFor)
        {
            int[] lowestCostRoute = new int[graph.GraphOfNodes.Count + 1];
            float currentLowestCostRoute = 0;
            int timeToExecuteForInMilliSec = timeToExecuteFor * 1000;
            var now = DateTime.Now;

            while (DateTime.Now < now.AddMilliseconds(timeToExecuteForInMilliSec))
            {
                // This is duplicated in menu. Must be a better layout than copy paste
                int[] route = randomRouteGenerator.GenerateSingleRandomRoute(graph.GraphOfNodes.Count, 0);
                float costOfRoute = routeEvaluator.CalculateCostOfSingleRoute(route, graph);

                if(IsLowestCostOfRouteSoFar(currentLowestCostRoute, costOfRoute))
                {
                    lowestCostRoute = route;
                    currentLowestCostRoute = costOfRoute;
                }
            }

            return new KeyValuePair<int[], float>(lowestCostRoute, currentLowestCostRoute);
        }

        private bool IsLowestCostOfRouteSoFar(float currentLowestCostRoute, float costOfCurrentRoute)
        {
            if(currentLowestCostRoute == 0 || costOfCurrentRoute < currentLowestCostRoute)
            {
                return true;
            }

            return false;
        }
    }
}