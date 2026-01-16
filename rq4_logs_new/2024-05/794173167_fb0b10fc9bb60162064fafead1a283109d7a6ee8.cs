using NetTopologySuite.Geometries;

namespace WebGeoInfrastructure.Entities
{
    public class Locality
    {
        private int id;

        private string name;

        private Geometry location;

        private List<Shop> shops;

        private List<Storage> storages;

        private List<Routes> originList;

        private List<Routes> destinyList;


        public int Id { get { return id; } private set => id = value; }

        public string Name
        {
            get { return name; }
            private set
            {
                if (String.IsNullOrEmpty(value))
                {
                    throw new ArgumentNullException("Name of the locality can´t be null or empty");
                }
                name = value;
            }
        }
        public Geometry Location { get { return location; } private set { location = value; } }

        public List<Shop> Shops { get { return shops; } private set => shops = value; }

        public List<Storage> Storages { get { return storages; } private set => storages = value; }

        public List<Routes> OriginList { get { return originList; } private set => originList = value; }

        public List<Routes> DestinyList { get { return destinyList; } private set => destinyList = value; }

    }
}