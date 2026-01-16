using NetTopologySuite.Geometries;

namespace WebGeoInfrastructure.Helpers
{
    public class GeometryConverter
    {
        public static NetTopologySuite.Geometries.Geometry convertToGeometry(double x, double y)
        {
            // Cria um GeometryFactory, que é usado para criar diferentes tipos de geometrias
            GeometryFactory geometryFactory = new GeometryFactory();

            // Cria um objeto Coordinate com as coordenadas do DTO
            Coordinate coordinate = new Coordinate(x, y);

            // Cria um ponto usando o GeometryFactory e a Coordinate
            Point point = geometryFactory.CreatePoint(coordinate);

            // Retorna o ponto criado
            return point;
        }
    }
}