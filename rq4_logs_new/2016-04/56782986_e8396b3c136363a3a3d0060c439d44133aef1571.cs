using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JarakTerdekat
{
    class PlainNode
    {
        public string name;

        public List<string> availableNeighbors;

        public List<PlainNeighbor> NeighborCollection;

        public PlainNode()
        {
            this.availableNeighbors = new List<string>();
            this.NeighborCollection = new List<PlainNeighbor>();
        }
    }

    class PlainNeighbor
    {
        public string name;
        public double jarak;

        public PlainNeighbor(string name, double jarak)
        {
            this.name = name;
            this.jarak = jarak;
        }
    }
}