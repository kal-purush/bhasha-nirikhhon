using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JarakTerdekat
{
    partial class MainWindow
    {
        private List<int> getPathWithFloyd()
        {
            Floyd floyd = new Floyd();

            //Console.WriteLine("Algorithm: Floyd");

            double inf = double.PositiveInfinity;

            //Console.WriteLine(inf);

            var pathTable = new List<List<double>>();

            for (int i = 0; i < nodeCollection.Nodes.Count; i++)
            {
                pathTable.Add(new List<double>());

                var from = nodeCollection.Nodes[i];
                var fromNeighbors = from.neighborsCollection;

                for (int j = 0; j < nodeCollection.Nodes.Count; j++)
                {
                    var to = nodeCollection.Nodes[j];

                    if (to.name == from.name)
                    {
                        pathTable[i].Add(0);
                    }
                    else
                    {
                        var index = fromNeighbors.getNeighborIndexByName(to.name);
                        if (index != -1)
                        {
                            pathTable[i].Add(fromNeighbors.Nodes[index].jarak);
                        }
                        else
                        {
                            pathTable[i].Add(inf);
                        }
                    }
                }
            }

            floyd.init(pathTable, (nodeCollection.Nodes.Count));
            var fromIndex = nodeCollection.getIndexByName(cb_initialNode.Text);
            var toIndex = nodeCollection.getIndexByName(cb_endNode.Text);

            //if (fromIndex > toIndex)
            //{
            //    var temp = fromIndex;
            //    fromIndex = toIndex;
            //    toIndex = temp;
            //}

            List<int> hasil = floyd.calculateShortestPath(fromIndex, toIndex);

            totalJarak = floyd.totalJarak;

            return hasil;
        }
    }
}