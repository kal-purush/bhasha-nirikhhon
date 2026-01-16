using System.Collections.Generic;
using System.Linq;
using TravellingSalespersonProj.LocalSearchTutorial;

namespace TravellingSalespersonProj.EvolutionaryAlgorithms
{
    public class EvolutionaryAlgorithmController
    {
        private readonly RandomRouteGenerator randomRouteGenerator;
        private readonly RouteEvaluator routeEvaluator;
        private List<Route> populationOne;
        private List<Route> populationTwo;
        private bool isUsingPopulationOne;
        

        public Dictionary<int, Route> BestRouteInGeneration { get; set; }

        public EvolutionaryAlgorithmController()
        {
            randomRouteGenerator = new RandomRouteGenerator();
            routeEvaluator = new RouteEvaluator();
            populationOne = new List<Route>();
            populationTwo = new List<Route>();
            isUsingPopulationOne = true;
            BestRouteInGeneration = new Dictionary<int, Route>();
            
        }

        public Route RunEvolutionaryAlgorithm(int sizeOfPopulation, int numberOfNodes, int startingNode, Graph graph)
        {
            AddInitialBestRouteForGenerationZero(numberOfNodes, startingNode, graph);
            this.populationOne = InitialisePoplulation(sizeOfPopulation, numberOfNodes, startingNode, graph);

            bool terminationCondition = true;

            while (terminationCondition)
            {

            }

            return null;
        }

        private List<Route> InitialisePoplulation(int sizeOfPopulation, int numberOfNodes, int startingNode, Graph graph)
        {
            List<Route> tempPopulation = new List<Route>();

            for(int index = 0; index < sizeOfPopulation; index++)
            {
                int[] routeIds = randomRouteGenerator.GenerateSingleRandomRoute(numberOfNodes, startingNode);
                float routeCost = routeEvaluator.CalculateCostOfSingleRoute(routeIds, graph);

                Route currentRoute = new Route(routeIds, routeCost);

                Route currentBestRoute = BestRouteInGeneration.ElementAt(0).Value;
                if (currentRoute.RouteCost < currentBestRoute.RouteCost)
                {
                    BestRouteInGeneration.Add(0, currentRoute);
                }

                tempPopulation.Add(currentRoute);
            }

            return tempPopulation;
        }

        private void AddInitialBestRouteForGenerationZero(int numberOfNodes, int startingNode, Graph graph)
        {
            // Adds a random initial best route for the generation
            int[] tempRoute = randomRouteGenerator.GenerateSingleRandomRoute(numberOfNodes, startingNode);
            float tempRouteCost = routeEvaluator.CalculateCostOfSingleRoute(tempRoute, graph);
            BestRouteInGeneration.Add(0, new Route(tempRoute, tempRouteCost));
        }
    }
}