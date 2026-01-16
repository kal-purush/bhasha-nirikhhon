using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JarakTerdekat
{
    class NodeCollection
    {
        public List<Node> Nodes;

        public NodeCollection()
        {
            this.Nodes = new List<Node>();
        }

        public void addNode(Node newNode)
        {
            Nodes.Add(newNode);
            Nodes[Nodes.Count - 1].allNodes = Nodes;
            addPreviousNodesToAvailableNeighobor();
            addToNodesAvailableNeigbor(Nodes.Count - 1);
        }

        public bool isContainsNode(string name)
        {
            foreach(var node in Nodes)
            {
                if(node.name == name)
                {
                    return true;
                }
            }
            return false;
        }

        public int getIndexByName(string name)
        {
            int index = -1;
            int count = 0;
            foreach (var node in Nodes)
            {
                if (node.name == name)
                {
                    index = count;
                    break;
                }
                count++;
            }
            return index;
        }

        protected void addPreviousNodesToAvailableNeighobor()
        {
            for(int i=0; i<Nodes.Count-1; i++)
            {
                Nodes[Nodes.Count - 1].availableNeighbors.Add(Nodes[i]);
            }
        }

        protected void addToNodesAvailableNeigbor(int index)
        {
            foreach(var node in Nodes)
            {
                if (node.name != Nodes[index].name)
                {
                    node.availableNeighbors.Add(Nodes[Nodes.Count - 1]);
                }
            }
        }
    }
}