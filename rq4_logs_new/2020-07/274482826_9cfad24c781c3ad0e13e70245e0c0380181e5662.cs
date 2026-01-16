using Microsoft.Xna.Framework;
using System;
using System.Collections.Generic;

namespace PixelHunter1995
{
    class PolygonPartition
    {
        public List<Polygon> Polygons { get; }
        public List<List<int>> Adjacents { get; }

        public PolygonPartition(List<Polygon> polygons)
        {
            Polygons = polygons;

            Adjacents = new List<List<int>>();
            for (int i = 0; i < polygons.Count; i++)
            {
                Adjacents.Add(new List<int>());
                for (int j = 0; j < polygons.Count; j++)
                {
                    if (i == j)
                    {
                        continue;
                    }

                    if (polygons[i].HasCommonEdgeWith(polygons[j]))
                    {
                        Adjacents[i].Add(j);
                    }
                }
            }
        }

        public PolygonPartition RemoveUnnecessaryEdges()
        {
            List<Polygon> newPolygons = new List<Polygon>(Polygons);

            for (int i = 0; i < Polygons.Count; i++)
            {
                foreach (int adjacentIndex in Adjacents[i])
                {
                    if (adjacentIndex < i)
                    {
                        continue;
                    }

                    Polygon currentPolygon = Polygons[i];
                    Polygon adjacentPolygon = Polygons[adjacentIndex];
                    Polygon combined = Polygons[i].CombineWith(Polygons[adjacentIndex]);
                    if (combined.IsConvex())
                    {
                        newPolygons.Remove(currentPolygon);
                        newPolygons.Remove(adjacentPolygon);
                        newPolygons.Add(combined);
                        return new PolygonPartition(newPolygons).RemoveUnnecessaryEdges();
                    }
                }
            }

            return new PolygonPartition(newPolygons);
        }

        public override string ToString()
        {
            return String.Join<Polygon>("; ", Polygons);
        }

        public void Draw(GraphicsDeviceManager graphics)
        {
            Polygons.ForEach(p => p.Draw(graphics));
        }
    }
}