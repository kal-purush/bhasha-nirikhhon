using Newtonsoft.Json;

namespace Assets.Scripts.Map
{
    enum TerrainType
    {
        grass = 0,
        water = 1,
        forest = 2,
        hills = 3
    }

    class EmptyField : MapObject
    {

    }

    class Castle : MapObject
    {

    }

    class Warrior : MapObject
    {

    }

    class Archer : MapObject
    {

    }

    class HorseMan : MapObject
    {

    }

    class GoldMine : MapObject
    {

    }

    [JsonConverter(typeof(MapObjectConverter))]
    abstract class MapObject
    {
        public int objType { get; set; }
        public bool fraction { get; set; }
        public string name { get; set; }
        public int health { get; set; }
        public int deffend { get; set; }
        public int attack { get; set; }
    }


    class MapElement
    {
        public TerrainType terrainType;
        public MapObject mapObject;
    }

    class Map
    {
        public int hight;
        public int width;
        public MapElement[] mapElements;
    }
}