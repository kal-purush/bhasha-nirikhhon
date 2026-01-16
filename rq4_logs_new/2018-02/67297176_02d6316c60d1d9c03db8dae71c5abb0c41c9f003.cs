namespace BooksHelixCharts.Utilities
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Windows.Media;
    using System.Windows.Media.Media3D;

    using HelixToolkit.Wpf;
    using BooksCore.Geography;
    using BooksUtilities.Geography;

    public class DiagramUtilities
    {
        private GeometryModel3D GetGeographyPlaneGeometry(CountryGeography geography, Color color, double opacity = 0.25)
        {
            int i = 0;
            var landBlocks = geography.LandBlocks.OrderByDescending(b => b.TotalArea);

            GeometryModel3D countryGeometry = new GeometryModel3D();

            var mapColor = new Color() { A = color.A, R = color.R, G = color.G, B = color.B };
            var material = MaterialHelper.CreateMaterial(mapColor, opacity);
            countryGeometry.Material = material;
            countryGeometry.BackMaterial = material;

            var geographyBuilder = new MeshBuilder(false, false);

            foreach (var boundary in landBlocks)
            {
                List<List<Point3D>> simpleAreaSet = SimplifyBoundary(boundary);

                foreach (var simpleArea in simpleAreaSet)
                {
                    var areaPts = new List<Point3D>() { };

                    foreach (var vertex in simpleArea)
                        areaPts.Add(new Point3D(vertex.X, vertex.Y, 0));

                    Point3DCollection area = new Point3DCollection(areaPts);
                    Polygon3D poly3D = new Polygon3D(area);
                    geographyBuilder.AddPolygon(area);
                }

                // just do the 10 biggest bits per country (looks to be enough)
                i++;
                if (i > 10)
                    break;
            }
            countryGeometry.Geometry = geographyBuilder.ToMesh();
            return countryGeometry;
        }

        private List<List<Point3D>> SimplifyBoundary(PolygonBoundary boundary)
        {
            var points = boundary.Points;

            List<Point3D> xyPoints = new List<Point3D>();
            foreach (var point in points)
            {
                double ptX = 0;
                double ptY = 0;
                point.GetCoordinates(out ptX, out ptY);
                Point3D dataPoint = new Point3D(ptX, ptY, 0);
                xyPoints.Add(dataPoint);
            }

            // need to turn these into a set of triangles
            List<List<Point3D>> simpleAreaSet =
                PolygonSimplifier.TriangulatePolygon(xyPoints);
            return simpleAreaSet;
        }
    }
}