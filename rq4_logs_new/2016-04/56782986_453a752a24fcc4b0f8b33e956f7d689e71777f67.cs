using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JarakTerdekat.Algorithm.BellmanFord
{
    class BellmanFordAlgorithm
    {
        public bool BellmanFord(ref int[] pi, ref List<Node> G, int s)
        {
            InitializeSingleSource(ref pi, ref G, s);

            for (int i = 0; i < G.Count - 1; i++)
            {
                Node u = G[i];

                for (int j = 0; j < u.Adjacency.Count; j++)
                {
                    Node v = u.Adjacency[j];
                    int w = u.Weights[j];

                    Relax(ref pi, u, ref v, w);

                    for (int k = 0; k < v.Adjacency.Count; k++)
                    {
                        Node x = v.Adjacency[k];

                        w = v.Weights[k];
                        Relax(ref pi, v, ref x, w);
                    }
                }
            }

            for (int i = 0; i < G.Count; i++)
            {
                Node u = G[i];

                for (int j = 0; j < u.Adjacency.Count; j++)
                {
                    Node v = u.Adjacency[j];
                    int w = u.Weights[j];

                    if (v.Distance > u.Distance + w)
                        return false;
                }
            }

            return true;
        }

        private void InitializeSingleSource(ref int[] pi, ref List<Node> nodeList, int s)
        {
            pi = new int[nodeList.Count];

            for (int i = 0; i < pi.Length; i++)
                pi[i] = -1;

            nodeList[s].Distance = 0;
        }

        private void Relax(ref int[] pi, Node u, ref Node v, int w)
        {
            if (v.Distance > u.Distance + w)
            {
                v.Distance = u.Distance + w;
                pi[v.Id] = u.Id;
            }
        }
    }

    class Node
    {
        int distance, id;
        string name;
        List<int> weights;
        List<Node> adjacency;

        public Node(int distance, int id, string name)
        {
            this.distance = distance;
            this.id = id;
            this.name = name;
            weights = new List<int>();
            adjacency = new List<Node>();
        }

        public int Distance
        {
            get
            {
                return distance;
            }
            set
            {
                distance = value;
            }
        }

        public int Id
        {
            get
            {
                return id;
            }
            set
            {
                id = value;
            }
        }

        public string Name
        {
            get
            {
                return name;
            }
            set
            {
                name = value;
            }
        }

        public List<Node> Adjacency
        {
            get
            {
                return adjacency;
            }
            set
            {
                adjacency = value;
            }
        }

        public List<int> Weights
        {
            get
            {
                return weights;
            }
            set
            {
                weights = value;
            }
        }
    }
}