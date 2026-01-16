/** 
 * @title A Simulation of LQueue Algorithm (The graph growth algorithm implemented with a queue)
 * @author Mohammad Andri Budiman <mandrib@gmail.com> and Dhika Handayani Rangkuti <dhikahandayani12@gmail.com>
 * @reference Gallo, Giorgio, and Stefano Pallottino. "Shortest path algorithms." Annals of Operations Research 13.1 (1988): 1-79.
 * @version 0.71
 * @since 2016-05-10
 */

using System;
using JarakTerdekat.Classes;
using System.Collections.Generic;

namespace JarakTerdekat
{
    public class LQueueBaru
    {
        public Dictionary<int, Dictionary<int, double>> graph;
        Dictionary<int, double> shortestDistances = new Dictionary<int, double>();
        Dictionary<int, double> predecessorVertex = new Dictionary<int, double>();

        public double totalJarak;
        public List<int> path;
        Stopwatch watch = new Stopwatch();
        public double elapsedTimeMs = 0;

        public LQueueBaru(Dictionary<int, Dictionary<int, double>> graph)
        {
            this.graph = graph;
            path = new List<int>();
        }


        public void LQueue(int startIndex, int toIndex)
        {
            watch.start();
            path.Add(toIndex);

            double INF = double.PositiveInfinity;
            Queue<int> queue = new Queue<int>();

            foreach (var pair in graph)
            {
                int node = pair.Key;
                shortestDistances.Add(node, INF);
                predecessorVertex.Add(node, -1);
            }

            shortestDistances[startIndex] = 0;
            queue.Enqueue(startIndex);

            while (queue.Count != 0)
            {
                int u = queue.Dequeue();
                foreach (var pair in graph[u])
                {
                    int v = pair.Key;
                    if (shortestDistances[v] > shortestDistances[u] + graph[u][v])
                    {
                        shortestDistances[v] = shortestDistances[u] + graph[u][v];
                        predecessorVertex[v] = u;
                        if (!queue.Contains(v))
                            queue.Enqueue(v);
                    }
                }
            }

            getPath(startIndex, toIndex);
            totalJarak = shortestDistances[toIndex];
            elapsedTimeMs = watch.stop();

            Console.WriteLine("\nDistance:");
            foreach (var pair in shortestDistances)
                Console.WriteLine("{0} : {1}", pair.Key, pair.Value);

            Console.WriteLine("\nPredecessor:");
            foreach (var pair in predecessorVertex)
                Console.WriteLine("{0} : {1}", pair.Key, pair.Value);
        }

        public void getPath(int u, int v)
        {
            double k;

            k = predecessorVertex[v];

            if (k == -1 || u == v)
            {
                return;
            }

            path.Add((int)k);
            getPath(u, (int)k);

        }
    }
}