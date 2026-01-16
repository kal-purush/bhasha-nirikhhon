using System.Collections.Generic;
using System.Linq;
using Code.PriorityQueues;

namespace Code.Graphs
{
    public class DijkstraSp
    {
        public double[] DistTo => _distTo;
        public string[] EdgeTo => _edgeTo.Select(s => s?.ToString()).ToArray();

        private readonly DirectedEdge[] _edgeTo;
        private readonly double[] _distTo;
        private readonly IndexMinPq<double> _pq;
        private readonly EdgeWeightedDigraph _g;
        
        public DijkstraSp(EdgeWeightedDigraph g, int s)
        {
            _g = g;
            _edgeTo = new DirectedEdge[g.V];
            _distTo = new double[g.V];
            _pq = new IndexMinPq<double>(g.V);

            for (int v = 0; v < g.V; v++)
            {
                _distTo[v] = double.PositiveInfinity;
            }
            _distTo[s] = 0;

            _pq.Insert(s, 0.0);
            while (!_pq.IsEmpty)
            {
                int v = _pq.DelMin();
                Relax(v);
            }
        }

        public double GetDistTo(int v)
        {
            return _distTo[v];
        }

        public bool HasPathTo(int v)
        {
            return _distTo[v] < double.PositiveInfinity;
        }

        public IEnumerable<DirectedEdge> GetPathTo(int v)
        {
            if (!HasPathTo(v)) return null;
            Stack<DirectedEdge> path = new Stack<DirectedEdge>();
            for (DirectedEdge e = _edgeTo[v]; e != null; e = _edgeTo[e.From])
            {
                path.Push(e);
            }

            return path.AsEnumerable();
        }

        private void Relax(int v)
        {
            foreach (DirectedEdge e in _g.GetAdjList(v))
            {
                int w = e.To;
                if (_distTo[w] > _distTo[v] + e.Weight)
                {
                    _distTo[w] = _distTo[v] + e.Weight;
                    _edgeTo[w] = e;
                    if (_pq.Contains(w))
                    {
                        _pq.ChangeKey(w, _distTo[w]);
                    }
                    else
                    {
                        _pq.Insert(w, _distTo[w]);
                    }
                }
            }
        }
    }
}