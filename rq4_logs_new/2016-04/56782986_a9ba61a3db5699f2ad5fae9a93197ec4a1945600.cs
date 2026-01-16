using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JarakTerdekat
{
    /*
    * This class will allow us to find, if any, a negative cycle
    * for a given edge weighted directed graph
    * */
    // find the shortest path from start to all destination
    // works for directed/nondirected graph
    public class BellmanFord
    {
        public List<edge> Edge = new List<edge>();
        public int V;   // number of vertex

        public BellmanFord()
        {
            shortestPathList = new List<int>();
        }

        public class edge
        {
            public int from, to;
            public double cost;
            public edge(int _from, int _to, double _cost)
            {
                from = _from;
                to = _to;
                cost = _cost;
            }
        }

        private double GetTotalPositiveCost()
        {
            double sum = 0;
            foreach (var e in Edge)
            {
                if (e.cost > 0) sum += e.cost;
            }
            return sum;
        }

        private void generateV()
        {
            foreach (var e in Edge)
            {
                V = Math.Max(V, e.from);
                V = Math.Max(V, e.to);
            }
            V++;
        }

        public int tempVertex;

        /// <summary>
        ///  return shortestPath[V] represents distance from startIndex
        /// </summary>
        public double[] GetShortestPath(int startIndex, int toIndex)
        {
            if (V == 0 && Edge.Count > 0) generateV();

            double[] shortestPath = new double[V];
            double INF = this.GetTotalPositiveCost() + 1;

            for (int i = 0; i < V; i++) shortestPath[i] = INF;

            shortestPath[startIndex] = 0;
            while (true)
            {
                bool update = false;
                foreach (edge e in Edge)
                {
                    if (shortestPath[e.from] != INF && shortestPath[e.to] > shortestPath[e.from] + e.cost)
                    {
                        shortestPath[e.to] = shortestPath[e.from] + e.cost;

                        update = true;

                        if(e.to == toIndex)
                        {
                            tempVertex = e.from;
                        }
                    }
                }
                if (!update) break;
            }

            return shortestPath;
        }

        public List<int> shortestPathList;

        private bool isNewSession = true;

        public void getShortestPathList(int startIndex, int toIndex)
        {
            if (isNewSession)
            {
                shortestPathList.Add(toIndex);
                isNewSession = false;
            }

            GetShortestPath(startIndex, toIndex);

            if (startIndex == toIndex)
            {
                isNewSession = true;
                return;
            }

            shortestPathList.Add(tempVertex);

            getShortestPathList(startIndex, tempVertex);
        }

        /// <summary>
        ///  return true if it has negative close loop
        /// </summary>
        public bool HasNegativeLoop()
        {
            double[] d = new double[V];
            for (int i = 0; i < V; i++)
            {
                foreach (edge e in Edge)
                {
                    if (d[e.to] > d[e.from] + e.cost)
                    {
                        d[e.to] = d[e.from] + e.cost;
                        if (i == V - 1) return true;
                    }
                }
            }
            return false;
        }
    }
}