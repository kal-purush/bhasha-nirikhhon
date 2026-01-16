using System.Collections.Generic;
using API_Graphs.Objects;

namespace API_Graphs
{
    /// <summary>
    /// La clase DijkstraResult representa el resultado dado por el metodo Dijkstra.
    /// </summary>
    /// Ver <see cref="Dijkstra"/> para la clase con el metodo.
    public class DijkstraResult
    {
        private int distance = 0;
        private List<Node> path = new List<Node>();
        public int Distance
        {
            get { return this.distance; }
            set { this.distance = value; }
        }
        public List<Node> Path
        {
            get { return this.path; }
            set { this.path = value; }
        }
    }
}